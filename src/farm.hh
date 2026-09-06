#pragma once
// niks3 claim stream and niks3-hook socket clients.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <nix/util/error.hh>
#include <nix/util/sync.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>
#include <nix/util/unix-domain-socket.hh>
#include <nix/util/util.hh>

namespace nixgrpc {

struct StaleClaim : nix::Error
{
    using nix::Error::Error;
};

class HookClient
{
    std::string socketPath;

    void send(const nlohmann::json & req) const
    {
        auto sock = nix::connect(std::filesystem::path{socketPath});
        nix::writeFull(sock.get(), req.dump() + "\n");
        auto resp = nlohmann::json::parse(nix::readLine(sock.get()), nullptr, /*allow_exceptions=*/false);
        auto status = resp.is_object() ? resp.value("status", "") : "";
        if (status == "stale") {
            throw StaleClaim("niks3-hook: claim superseded, outputs not published");
        }
        if (status != "ok") {
            throw nix::Error("niks3-hook: %s", resp.is_object() ? resp.value("message", "push failed") : "bad reply");
        }
    }

public:
    explicit HookClient(std::string socketPath)
        : socketPath(std::move(socketPath))
    {
    }

    void queue(const std::vector<std::string> & paths) const
    {
        send({{"paths", paths}});
    }

    // Returns after niks3 committed. StaleClaim if the token was superseded.
    void pushWait(const std::vector<std::string> & paths, int64_t claimToken) const
    {
        send({{"paths", paths}, {"wait", true}, {"claim_token", claimToken}});
    }
};

namespace http {

constexpr long conflict = 409;

inline auto ok(long status) -> bool
{
    constexpr long first = 200;
    constexpr long last = 299;
    return status >= first && status <= last;
}

// NOLINTBEGIN(cppcoreguidelines-pro-type-vararg): curl_easy_* are vararg.
// One prepared request. Callbacks run inside C frames and must not throw.
class Call
{
    struct CurlDeleter
    {
        void operator()(CURL * handle) const noexcept
        {
            curl_easy_cleanup(handle);
        }
    };

    struct SlistDeleter
    {
        void operator()(curl_slist * list) const noexcept
        {
            curl_slist_free_all(list);
        }
    };

    std::unique_ptr<CURL, CurlDeleter> curl;
    std::unique_ptr<curl_slist, SlistDeleter> headers;
    std::string url;
    std::string payload;
    std::string body_;

    static auto collect(char * data, size_t size, size_t nmemb, void * userp) noexcept -> size_t
    {
        try {
            static_cast<std::string *>(userp)->append(data, size * nmemb);
            return size * nmemb;
        } catch (...) {
            return 0;
        }
    }

    void header(const std::string & line)
    {
        auto * next = curl_slist_append(headers.get(), line.c_str());
        if (next == nullptr) {
            throw nix::Error("curl_slist_append failed");
        }
        auto * prev = headers.release(); // owned via next
        static_cast<void>(prev);
        headers.reset(next);
    }

public:
    template<typename T>
    void opt(CURLoption option, T value)
    {
        if (curl_easy_setopt(curl.get(), option, value) != CURLE_OK) {
            throw nix::Error("curl_easy_setopt %d failed", static_cast<int>(option));
        }
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    Call(std::string url_, const std::string & bearer, const std::optional<nlohmann::json> & body)
        : url(std::move(url_))
    {
        static std::once_flag once;
        std::call_once(once, []() -> void { curl_global_init(CURL_GLOBAL_DEFAULT); });
        curl.reset(curl_easy_init());
        if (!curl) {
            throw nix::Error("curl_easy_init failed");
        }
        constexpr long timeoutSecs = 30;
        header("Authorization: Bearer " + bearer);
        if (body) {
            payload = body->dump();
            header("Content-Type: application/json");
            opt(CURLOPT_POSTFIELDSIZE, static_cast<long>(payload.size()));
            opt(CURLOPT_POSTFIELDS, payload.c_str());
        }
        opt(CURLOPT_URL, url.c_str());
        opt(CURLOPT_HTTPHEADER, headers.get());
        opt(CURLOPT_NOSIGNAL, 1L);
        opt(CURLOPT_TIMEOUT, timeoutSecs);
        opt(CURLOPT_WRITEFUNCTION, &Call::collect);
        opt(CURLOPT_WRITEDATA, &body_);
    }

    // Returns the HTTP status. Throws on transport errors.
    auto perform() -> long
    {
        auto code = curl_easy_perform(curl.get());
        if (code != CURLE_OK && code != CURLE_WRITE_ERROR && code != CURLE_ABORTED_BY_CALLBACK) {
            throw nix::Error("%s: %s", url, curl_easy_strerror(code));
        }
        long status = 0;
        curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status);
        return status;
    }

    [[nodiscard]] auto body() const -> const std::string &
    {
        return body_;
    }

    [[nodiscard]] auto retryAfter() const -> std::chrono::seconds
    {
        curl_off_t wait = 0;
        curl_easy_getinfo(curl.get(), CURLINFO_RETRY_AFTER, &wait);
        return std::chrono::seconds(std::max<curl_off_t>(wait, 0));
    }
};
// NOLINTEND(cppcoreguidelines-pro-type-vararg)

} // namespace http

// The stream runs on its own thread and is re-opened with our token when it
// breaks. Three heartbeats of silence end it.
class Claim
{
public:
    enum class Status : uint8_t { pending, wait, build, built, failed };

private:
    using Clock = std::chrono::steady_clock;

    // Immutable after construction, read by the stream thread without locking.
    const std::string baseUrl;
    const std::string bearer;
    const Clock::duration heartbeat;

    struct State
    {
        nlohmann::json request; // gains "token" once granted
        Status status = Status::pending;
        int64_t token = 0;
        std::string kind;
        Clock::time_point lastSeen = Clock::now();
        std::chrono::seconds retryAfter{};
        bool stop = false;

        [[nodiscard]] auto decided() const -> bool
        {
            return status == Status::built || status == Status::failed;
        }

        // A waiter losing niks3 costs nothing, so it may back off for a long
        // time. A holder is fenced after 3 heartbeats and must not outlive that.
        [[nodiscard]] auto giveUpAfter(Clock::duration heartbeat) const -> Clock::duration
        {
            constexpr int waiterPatience = 60;
            return (status == Status::build ? 3 : waiterPatience) * heartbeat;
        }
    };

    nix::Sync<State> state_;
    std::condition_variable cv;

    // Declared last: starts after, and is joined before, everything above.
    std::thread streamThread;

    void onLine(std::string_view line)
    {
        auto msg = nlohmann::json::parse(line, nullptr, /*allow_exceptions=*/false);
        if (!msg.is_object()) {
            return;
        }
        auto status = msg.value("status", "");
        auto tok = msg.value("token", int64_t{0});
        auto state(state_.lock());
        state->lastSeen = Clock::now();
        if (status == "build" && tok > 0) {
            state->status = Status::build;
            state->token = tok;
            state->request["token"] = tok; // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): json object insert
        } else if (status == "wait" && state->status == Status::pending) {
            state->status = Status::wait;
        } else if (status == "built") {
            state->status = Status::built;
        } else if (status == "failed") {
            state->status = Status::failed;
            state->kind = msg.value("kind", "");
        }
        cv.notify_all();
    }

    struct StreamCtx
    {
        Claim * self;
        std::string buf;
    };

    static auto onData(char * data, size_t size, size_t nmemb, void * userp) noexcept -> size_t
    {
        constexpr size_t maxLine = size_t{64} * 1024;
        try {
            auto & ctx = *static_cast<StreamCtx *>(userp);
            if (ctx.self->state_.lock()->stop) {
                return 0;
            }
            ctx.buf.append(data, size * nmemb);
            for (auto pos = ctx.buf.find('\n'); pos != std::string::npos; pos = ctx.buf.find('\n')) {
                ctx.self->onLine(std::string_view(ctx.buf).substr(0, pos));
                ctx.buf.erase(0, pos + 1);
            }
            return ctx.buf.size() > maxLine ? 0 : size * nmemb;
        } catch (...) {
            return 0;
        }
    }

    static auto onProgress(
        void * userp, curl_off_t /*dlt*/, curl_off_t /*dln*/, curl_off_t /*ult*/, curl_off_t /*uln*/) noexcept
        -> int
    {
        auto & self = *static_cast<Claim *>(userp);
        auto state(self.state_.lock());
        return state->stop || Clock::now() - state->lastSeen > state->giveUpAfter(self.heartbeat) ? 1 : 0;
    }

    void runStream()
    {
        std::optional<nlohmann::json> const body = state_.lock()->request;
        StreamCtx ctx{.self = this, .buf = {}};
        http::Call call(baseUrl + "/api/builds/claim", bearer, body);
        call.opt(CURLOPT_TIMEOUT, 0L);
        call.opt(CURLOPT_WRITEFUNCTION, &Claim::onData);
        call.opt(CURLOPT_WRITEDATA, &ctx);
        call.opt(CURLOPT_NOPROGRESS, 0L);
        call.opt(CURLOPT_XFERINFOFUNCTION, &Claim::onProgress);
        call.opt(CURLOPT_XFERINFODATA, this);
        call.perform();
        state_.lock()->retryAfter = call.retryAfter();
    }

    static auto jitter(Clock::duration base) -> Clock::duration
    {
        thread_local std::minstd_rand rng{std::random_device{}()};
        return base / 2 + Clock::duration(std::uniform_int_distribution<Clock::rep>(0, base.count())(rng)) / 2;
    }

    template<typename Pred>
    auto waitFor(Pred done) -> Status
    {
        auto state(state_.lock());
        while (!done(*state) && !state->stop) {
            state.wait(cv);
        }
        if (!done(*state)) {
            throw nix::Error("niks3 claim stream lost before a decision");
        }
        return state->status;
    }

    void halt()
    {
        state_.lock()->stop = true;
        cv.notify_all();
    }

    void run() noexcept
    {
        constexpr int maxBackoffHeartbeats = 8;
        auto backoff = heartbeat / 2;
        while (!state_.lock()->stop) {
            auto started = Clock::now();
            try {
                runStream();
            } catch (...) {
                nix::ignoreExceptionExceptInterrupt();
            }
            auto state(state_.lock());
            if (state->stop || state->decided() || Clock::now() - state->lastSeen > state->giveUpAfter(heartbeat)) {
                break;
            }
            backoff = Clock::now() - started > heartbeat ? heartbeat / 2 : std::min(backoff * 2, maxBackoffHeartbeats * heartbeat);
            auto pause = std::max<Clock::duration>(jitter(backoff), state->retryAfter);
            if (state->status == Status::build) {
                pause = std::min(pause, heartbeat);
            }
            state.wait_for(cv, pause, [&]() -> bool { return state->stop; });
        }
        halt();
    }

public:
    Claim(std::string baseUrl, std::string bearer, Clock::duration heartbeat, nlohmann::json request)
        : baseUrl(std::move(baseUrl))
        , bearer(std::move(bearer))
        , heartbeat(heartbeat)
        , state_(State{.request = std::move(request)})
        , streamThread([this]() -> void { run(); })
    {
    }

    Claim(const Claim &) = delete;
    Claim(Claim &&) = delete;
    auto operator=(const Claim &) -> Claim & = delete;
    auto operator=(Claim &&) -> Claim & = delete;

    // Closing the stream is enough: niks3 releases a held claim when the
    // holder's connection drops, so early returns need no explicit fail().
    ~Claim()
    {
        halt();
        if (streamThread.joinable()) {
            streamThread.join();
        }
    }

    // First word from niks3, possibly `wait`.
    auto first() -> Status
    {
        return waitFor([](const State & state) -> bool { return state.status != Status::pending; });
    }

    // build, built or failed.
    auto await() -> Status
    {
        return waitFor([](const State & state) -> bool { return state.status == Status::build || state.decided(); });
    }

    auto status() -> Status
    {
        return state_.lock()->status;
    }

    auto token() -> int64_t
    {
        return state_.lock()->token;
    }

    auto kind() -> std::string
    {
        return state_.lock()->kind;
    }

    // Held, then silent for 3 heartbeats: abandon the build.
    auto lost() -> bool
    {
        auto state(state_.lock());
        return state->status == Status::build && state->stop;
    }

    // Empty kind = transient. False on stale token.
    auto fail(const std::string & kind) -> bool
    {
        http::Call call(baseUrl + "/api/builds/fail", bearer, nlohmann::json{{"claim_token", token()}, {"kind", kind}});
        auto status = call.perform();
        halt();
        if (status == http::conflict) {
            return false;
        }
        if (!http::ok(status)) {
            throw nix::Error("niks3 fail: HTTP %d: %s", status, call.body());
        }
        return true;
    }
};

struct ClaimRequest
{
    std::vector<std::string> outputs;
    std::vector<std::string> inputs;
};

class Niks3
{
    std::string baseUrl;
    std::string bearer;
    std::chrono::milliseconds heartbeat{0};

public:
    Niks3(std::string url, std::string bearerToken)
        : baseUrl(std::move(url))
        , bearer(std::move(bearerToken))
    {
        while (baseUrl.ends_with('/')) {
            baseUrl.pop_back();
        }
        http::Call call(baseUrl + "/api/cache-config", bearer, std::nullopt);
        if (auto status = call.perform(); !http::ok(status)) {
            throw nix::Error("niks3 cache-config: HTTP %d", status);
        }
        auto cfg = nlohmann::json::parse(call.body(), nullptr, /*allow_exceptions=*/false);
        if (!cfg.is_object()) {
            throw nix::Error("niks3 cache-config: not a JSON object");
        }
        auto secs = cfg.value("claim_heartbeat_secs", 0.0);
        if (secs <= 0) {
            throw nix::Error("niks3 at %s does not support build claims", baseUrl);
        }
        heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration<double>(secs));
    }

    [[nodiscard]] auto claim(const ClaimRequest & req) const -> std::unique_ptr<Claim>
    {
        nlohmann::json body{{"outputs", req.outputs}};
        if (!req.inputs.empty()) {
            body.emplace("inputs", req.inputs);
        }
        return std::make_unique<Claim>(baseUrl, bearer, heartbeat, std::move(body));
    }
};

} // namespace nixgrpc
