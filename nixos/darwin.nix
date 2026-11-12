{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.nix-grpc-daemon;
  logDir = "/var/log/nix-grpc-daemon";
  logFile = "${logDir}/daemon.log";
  svidLog = "${logDir}/svid.log";
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
  # launchd has no ExecStartPre: the job waits for the files itself, and a
  # miss ends it so KeepAlive tries again.
  waitForSvid = pkgs.writeShellScript "nix-grpc-daemon-wait-svid" ''
    for _ in $(seq 60); do
      [ -s ${cfg.spire.directory}/svid.key ] && [ -s ${cfg.spire.directory}/bundle.pem ] && exec "$@"
      sleep 1
    done
    echo "no SVID in ${cfg.spire.directory} after 60 s" >&2
    exit 1
  '';
in
{
  imports = [ ./common.nix ];

  options.services.nix-grpc-daemon = {
    uid = lib.mkOption {
      type = lib.types.int;
      default = 450;
      description = "UID of the daemon user.";
    };
    gid = lib.mkOption {
      type = lib.types.int;
      default = 450;
      description = "GID of the daemon group.";
    };
  };

  config = lib.mkIf cfg.enable {
    services.nix-grpc-daemon.idleTimeout = lib.mkDefault null;
    # /run is cleared at boot and the activation script that creates the
    # directory runs only at a rebuild
    services.nix-grpc-daemon.spire.directory = lib.mkDefault "/var/lib/nix-grpc-daemon/svid";

    assertions = [
      {
        assertion = cfg.idleTimeout == null;
        message = "services.nix-grpc-daemon.idleTimeout needs socket activation, which launchd does not provide";
      }
    ];

    users.knownGroups = [ "nix-grpc-daemon" ];
    users.knownUsers = [ "nix-grpc-daemon" ];
    users.groups.nix-grpc-daemon.gid = cfg.gid;
    users.users.nix-grpc-daemon = {
      inherit (cfg) uid gid;
      home = "/var/empty";
      shell = "/usr/bin/false";
      isHidden = true;
    };

    launchd.daemons.nix-grpc-daemon-svid = lib.mkIf spireOn {
      serviceConfig = {
        ProgramArguments = [
          (lib.getExe cfg.spire.package)
          "-config"
          (toString spiffeHelperConf)
        ];
        UserName = "nix-grpc-daemon";
        GroupName = "nix-grpc-daemon";
        RunAtLoad = true;
        KeepAlive = true;
        StandardOutPath = svidLog;
        StandardErrorPath = svidLog;
      };
    };

    launchd.daemons.nix-grpc-daemon = {
      path = lib.optional (cfg.niks3 != null) config.nix.package;
      serviceConfig = {
        ProgramArguments =
          lib.optional spireOn (toString waitForSvid)
          ++ map toString (import ./daemon-args.nix { inherit cfg lib pkgs; });
        UserName = "nix-grpc-daemon";
        GroupName = "nix-grpc-daemon";
        RunAtLoad = true;
        KeepAlive = true;
        # launchd's default soft limit is 256, which a busy worker exhausts
        # with one socket per balancer connection plus the NAR spool files.
        SoftResourceLimits.NumberOfFiles = lib.mkDefault 65536;
        HardResourceLimits.NumberOfFiles = lib.mkDefault 65536;
        StandardOutPath = logFile;
        StandardErrorPath = logFile;
      };
    };

    # launchd opens the log as the job's user, so /var/log itself is not writable.
    system.activationScripts.postActivation.text = ''
      install -d -o nix-grpc-daemon -g nix-grpc-daemon -m 0750 ${logDir}
    ''
    + lib.optionalString spireOn ''
      install -d -o nix-grpc-daemon -g nix-grpc-daemon -m 0755 ${cfg.spire.directory}
    '';
    environment.etc."newsyslog.d/nix-grpc-daemon.conf".text = ''
      ${logFile} nix-grpc-daemon:nix-grpc-daemon 640 5 10240 * J
    ''
    + lib.optionalString spireOn ''
      ${svidLog} nix-grpc-daemon:nix-grpc-daemon 640 5 1024 * J
    '';
  };
}
