{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.nix-grpc-daemon;
  builder = lib.elem "builder" cfg.roles;
  # A socket unit is not restarted when its listen address changes, and an
  # idle node would never register with the scheduler.
  socketActivated = cfg.idleTimeout != null;
  spireOn = cfg.spire.agentSocket != null;
  # spiffe-helper renews the SVID at half its lifetime and rewrites the
  # files in place; the daemon re-reads them on its own.
  spiffeHelperConf = pkgs.writeText "nix-grpc-daemon-spiffe-helper.conf" ''
    agent_address = "${cfg.spire.agentSocket}"
    cert_dir = "${cfg.spire.directory}"
    svid_file_name = "svid.pem"
    svid_key_file_name = "svid.key"
    svid_bundle_file_name = "bundle.pem"
    daemon_mode = true
  '';
  waitForSvid = pkgs.writeShellScript "nix-grpc-daemon-wait-svid" ''
    for _ in $(seq 60); do
      [ -s ${cfg.spire.directory}/svid.key ] && [ -s ${cfg.spire.directory}/bundle.pem ] && exit 0
      sleep 1
    done
    echo "no SVID in ${cfg.spire.directory} after 60 s" >&2
    exit 1
  '';
in
{
  imports = [ ./common.nix ];

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.scheduler == null || cfg.idleTimeout == null;
        message = "services.nix-grpc-daemon.idleTimeout must be null with scheduler: an idle builder would stop taking work";
      }
    ];

    # gRPC clients inherit the store privileges of this uid via the proxied
    # nix-daemon connection, so default to a dedicated unprivileged user.
    users.users.nix-grpc-daemon = {
      isSystemUser = true;
      group = "nix-grpc-daemon";
    };
    users.groups.nix-grpc-daemon = { };

    systemd.sockets.nix-grpc-daemon = lib.mkIf socketActivated {
      description = "Nix worker-protocol over gRPC";
      wantedBy = [ "sockets.target" ];
      socketConfig.ListenStream = lib.removePrefix "unix:" cfg.listen;
    };

    systemd.services.nix-grpc-daemon-svid = lib.mkIf spireOn {
      description = "This node's SVID and trust bundle from the SPIRE agent";
      wantedBy = [ "multi-user.target" ];
      wants = [ "spire-agent.service" ];
      after = [ "spire-agent.service" ];
      serviceConfig = {
        Type = "exec";
        User = "nix-grpc-daemon";
        Group = "nix-grpc-daemon";
        RuntimeDirectory = lib.removePrefix "/run/" cfg.spire.directory;
        RuntimeDirectoryMode = "0755";
        ExecStart = "${lib.getExe cfg.spire.package} -config ${spiffeHelperConf}";
        Restart = "always";
        RestartSec = 5;
      };
    };

    systemd.services.nix-grpc-daemon = {
      description = "Nix worker-protocol over gRPC";
      # niks3 push shells out to `nix path-info`.
      path = lib.optional (cfg.niks3 != null) config.nix.package;
      wantedBy = lib.mkIf (!socketActivated) [ "multi-user.target" ];
      requires =
        lib.optional socketActivated "nix-grpc-daemon.socket"
        ++ lib.optional spireOn "nix-grpc-daemon-svid.service";
      # nix-daemon is socket-activated; ordering after the socket is enough,
      # the first proxied connection will start it.
      after =
        lib.optional socketActivated "nix-grpc-daemon.socket"
        ++ lib.optional spireOn "nix-grpc-daemon-svid.service"
        ++ [ "nix-daemon.socket" ];
      wants = [ "nix-daemon.socket" ];
      # Reload = drain: leave the balancer, finish and publish running
      # builds, then exit. The socket or Restart=always starts the new
      # generation.
      reloadIfChanged = builder;
      # /tmp is a size-limited tmpfs.
      environment.TMPDIR = "/var/cache/nix-grpc-daemon";
      serviceConfig = {
        CacheDirectory = "nix-grpc-daemon";
        LimitNOFILE = lib.mkDefault 65536;
        Type = "notify";
        WatchdogSec = 30;
        # Keep RPCs responsive next to builds in nix-daemon.service.
        CPUWeight = lib.mkDefault 1000;
        User = "nix-grpc-daemon";
        Group = "nix-grpc-daemon";
        Restart = if socketActivated then "on-failure" else "always";
        ExecStart = lib.escapeShellArgs (import ./daemon-args.nix { inherit cfg lib pkgs; });
        # Builds run in nix-daemon. This only bounds the proxy.
        MemoryMax = lib.mkDefault "2G";
      }
      // lib.optionalAttrs spireOn { ExecStartPre = waitForSvid; }
      // lib.optionalAttrs (!socketActivated) {
        # Binds listen itself, which may be a port below 1024.
        AmbientCapabilities = [ "CAP_NET_BIND_SERVICE" ];
        CapabilityBoundingSet = [ "CAP_NET_BIND_SERVICE" ];
      }
      // lib.optionalAttrs builder {
        ExecReload = "${pkgs.coreutils}/bin/kill -TERM $MAINPID";
        # stop drains too, and niks3 push must live on to publish.
        KillMode = "mixed";
        TimeoutStopSec = lib.mkDefault "1h";
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        RestrictAddressFamilies = [
          "AF_UNIX"
          "AF_INET"
          "AF_INET6"
        ];
      };
    };
  };
}
