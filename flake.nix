{
  description = "Blink LN Gateway";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem
    (system: let
      overlays = [
        (import rust-overlay)
      ];
      pkgs = import nixpkgs {
        inherit system overlays;
      };
      rustVersion = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      rustToolchain = rustVersion.override {
        extensions = ["rust-analyzer" "rust-src"];
      };
      nativeBuildInputs = with pkgs; [
        rustToolchain
        alejandra
        bats
        cargo-watch
        cargo-audit
        curl
        jq
        openssl
        postgresql
        rover
        tilt
        vendir
        typos
        protobuf
        ytt
      ];
    in
      with pkgs; {
        devShells.default = mkShell {
          inherit nativeBuildInputs;
        };

        formatter = alejandra;
      });
}
