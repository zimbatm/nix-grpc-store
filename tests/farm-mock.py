#!/usr/bin/env python3
"""Mock niks3 (HTTP) and niks3-hook (unix socket) for farm client tests.

State is in memory and single-process. Enough protocol to drive the C++
claim and hook clients: cache-config, NDJSON claim stream with heartbeats,
fail, and the hook's line-delimited JSON. A control endpoint lets the test
inject events (complete, fail, drop stream) and read what the mock saw.
"""

import json
import os
import select
import socket
import socketserver
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

HB = float(os.environ.get("FARM_MOCK_HB", "0.2"))


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.next_token = 1
        self.claims: dict[str, int] = {}  # key -> token
        self.built: set[str] = set()
        self.failed: dict[str, str] = {}  # key -> kind, one-shot for current waiters
        self.drop: set[str] = set()  # keys whose holder stream should be cut
        self.frozen = False  # niks3 "down": streams cut, new claims hang silently
        self.overload = 0  # next N claim requests get 503 Retry-After
        self.log: list[dict[str, Any]] = []

    def record(self, **ev: Any) -> None:
        with self.lock:
            self.log.append(ev)


S = State()


def key_of(outputs: list[str]) -> str:
    return "\n".join(sorted(outputs))


class Http(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_: Any) -> None:
        pass

    def body(self) -> Any:
        n = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(n) or b"{}")

    def reply(self, code: int, obj: Any = None) -> None:
        data = b"" if obj is None else json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/api/cache-config":
            self.reply(200, {"claim_heartbeat_secs": HB})
        elif self.path == "/_mock/log":
            with S.lock:
                self.reply(200, S.log)
        else:
            self.reply(404)

    def do_POST(self) -> None:
        auth = self.headers.get("Authorization", "")
        if self.path.startswith("/api/") and auth != "Bearer testtoken":
            self.reply(401)
            return
        req = self.body()
        if self.path == "/api/builds/claim":
            self.claim(req)
        elif self.path == "/api/builds/fail":
            self.fail(req)
        elif self.path == "/_mock/complete":
            with S.lock:
                k = key_of(req["outputs"])
                S.claims.pop(k, None)
                S.built.add(k)
                S.cv.notify_all()
            self.reply(204)
        elif self.path == "/_mock/overload":
            with S.lock:
                S.overload = req["n"]
            self.reply(204)
        elif self.path == "/_mock/freeze":
            with S.lock:
                S.frozen = req.get("frozen", True)
                S.cv.notify_all()
            self.reply(204)
        elif self.path == "/_mock/drop":
            with S.lock:
                S.drop.add(key_of(req["outputs"]))
                S.cv.notify_all()
            self.reply(204)
        else:
            self.reply(404)

    def fail(self, req: dict[str, Any]) -> None:
        with S.lock:
            k = next((k for k, t in S.claims.items() if t == req["claim_token"]), None)
            if k is None:
                self.reply(409)
                return
            del S.claims[k]
            if req.get("kind"):
                S.failed[k] = req["kind"]
            S.cv.notify_all()
        S.record(ev="fail", token=req["claim_token"], kind=req.get("kind", ""))
        self.reply(204)

    def send_line(self, obj: dict[str, Any]) -> None:
        line = json.dumps(obj).encode() + b"\n"
        self.wfile.write(b"%x\r\n%s\r\n" % (len(line), line))
        self.wfile.flush()

    def claim(self, req: dict[str, Any]) -> None:
        k = key_of(req["outputs"])
        S.record(ev="claim", outputs=req["outputs"], token=req.get("token", 0))
        if S.frozen:
            time.sleep(10 * HB)
            self.reply(503)
            return
        with S.lock:
            shed = S.overload > 0
            S.overload -= shed
        if shed:
            S.record(ev="shed")
            self.send_response(503)
            self.send_header("Retry-After", "1")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        sent_wait = False
        try:
            while True:
                with S.lock:
                    if k in S.built:
                        st: dict[str, Any] | None = {"status": "built"}
                    elif k in S.failed:
                        st = {"status": "failed", "kind": S.failed.pop(k)}
                    elif k not in S.claims or S.claims[k] == req.get("token"):
                        tok = S.claims.get(k) or S.next_token
                        if k not in S.claims:
                            S.next_token += 1
                            S.claims[k] = tok
                        st = {"status": "build", "token": tok}
                    else:
                        st = None
                if st is not None:
                    self.send_line(st)
                    if st["status"] == "build":
                        self.hold(k, st["token"])
                    break
                if not sent_wait:
                    self.send_line({"status": "wait"})
                    sent_wait = True
                with S.lock:
                    S.cv.wait(HB)
                self.send_line({"status": "hb"})
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            try:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except OSError:
                pass
            self.close_connection = True

    def hold(self, k: str, token: int) -> None:
        """Heartbeat until the client goes away, the claim is released, or the test drops us."""
        while True:
            with S.lock:
                if S.claims.get(k) != token:
                    return
                if k in S.drop or S.frozen:
                    S.drop.discard(k)
                    S.log.append({"ev": "dropped", "token": token})
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
            r, _, _ = select.select([self.connection], [], [], HB)
            try:
                if r and self.connection.recv(1, socket.MSG_PEEK) == b"":
                    break
                self.send_line({"status": "hb"})
            except OSError:
                break
        with S.lock:
            if S.claims.get(k) == token:
                del S.claims[k]
                S.log.append({"ev": "released_by_disconnect", "token": token})
                S.cv.notify_all()


class Hook(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        req = json.loads(self.rfile.readline())
        S.record(ev="hook", **req)
        if req.get("wait"):
            time.sleep(HB)
        if any("fail" in p for p in req["paths"]):
            resp = {"status": "error", "message": "push failed"}
        elif any("stale" in p for p in req["paths"]):
            resp = {"status": "stale", "message": "stale build claim"}
        else:
            resp = {"status": "ok"}
            tok = req.get("claim_token")
            with S.lock:
                k = next((k for k, t in S.claims.items() if t == tok), None)
                if k is not None:
                    del S.claims[k]
                    S.built.add(k)
                    S.cv.notify_all()
        self.wfile.write(json.dumps(resp).encode() + b"\n")


class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


def main() -> None:
    sock_path = sys.argv[1]
    http = ThreadingHTTPServer(("127.0.0.1", 0), Http)
    http.daemon_threads = True
    if os.path.exists(sock_path):
        os.unlink(sock_path)
    hook = UnixServer(sock_path, Hook)
    threading.Thread(target=hook.serve_forever, daemon=True).start()
    print(http.server_address[1], flush=True)
    http.serve_forever()


if __name__ == "__main__":
    main()
