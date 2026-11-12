# The balancer with its secrets from a SPIRE agent: envoy accepts the
# rendered config. No agent runs here, so the SDS fetches time out at
# runtime; validation only checks the shape.
{ pkgs, lbModule }:
let
  inherit (pkgs) lib;
  envoy =
    (lib.evalModules {
      modules = [
        lbModule
        (
          { lib, ... }:
          {
            options.services.envoy = lib.mkOption { type = lib.types.anything; };
            options.systemd = lib.mkOption { type = lib.types.anything; };
            options.assertions = lib.mkOption { type = lib.types.anything; };
            options.warnings = lib.mkOption { type = lib.types.anything; };
            config._module.args.pkgs = pkgs;
            config.services.nix-grpc-farm-lb = {
              enable = true;
              systems = [
                "x86_64-linux"
                "aarch64-linux"
              ];
              scheduler = "192.0.2.1:50051";
              tls = {
                # the public certificate stays a file; the check makes one
                certFile = "/build/tls/lb/tls.crt";
                keyFile = "/build/tls/lb/tls.key";
                spire = {
                  agentSocket = "/run/spire/sockets/agent.sock";
                  clients = true;
                  upstream = "spiffe://example.org/farm/worker/";
                };
              };
            };
          }
        )
      ];
    }).config;
  failed = lib.filter (a: !a.assertion) envoy.assertions;
  # SDS over gRPC needs the node identity; `admin` stays out (mkIf-wrapped).
  config = (pkgs.formats.json { }).generate "envoy-lb-spire.json" {
    inherit (envoy.services.envoy.settings) node static_resources;
  };
in
assert lib.assertMsg (failed == [ ]) (lib.concatMapStringsSep "\n" (a: a.message) failed);
pkgs.runCommand "lb-spire-check"
  {
    nativeBuildInputs = [
      pkgs.envoy-bin
      pkgs.openssl
    ];
  }
  ''
    mkdir -p /build/tls/lb
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=farm \
      -keyout /build/tls/lb/tls.key -out /build/tls/lb/tls.crt 2>/dev/null
    envoy --log-level error --mode validate -c ${config}
    # the SPIFFE ID of a client goes to the workers, the balancer's SVID to the workers
    grep -q '"uri": *true' ${config} || grep -q '"uri":true' ${config}
    grep -q '"ALL"' ${config}
    grep -q 'spiffe://example.org/farm/worker/' ${config}
    touch $out
  ''
