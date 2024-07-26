{
  description = "Build a cargo workspace";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";

    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.rust-overlay.follows = "rust-overlay";
    };

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-utils.url = "github:numtide/flake-utils";

    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, crane, fenix, flake-utils, advisory-db, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };

        inherit (pkgs) lib;

        rustToolchain = pkgs.rust-bin.stable."1.79.0".default;#//(pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml);
          # this is how we can tell crane to use our toolchain!
          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
          sqlFilter = path: _type: null != builtins.match ".*sql$" path;
        sqlOrCargo = path: type: (sqlFilter path type) || (craneLib.filterCargoSources path type);

        src = lib.cleanSourceWith {
          src = ./.; # The original, unfiltered source
          filter = sqlOrCargo;
          name = "source"; # Be reproducible, regardless of the directory name
        };

        # Common arguments can be set here to avoid repeating them later
        commonArgs = {
          inherit src;
          strictDeps = true;

          buildInputs = [
            pkgs.openssl
            pkgs.curl
            # Add additional build inputs here
          ] ++ lib.optionals pkgs.stdenv.isDarwin [
            # Additional darwin specific inputs can be set here
            pkgs.libiconv
          ];

          # Additional environment variables can be set directly
          # MY_CUSTOM_VAR = "some value";
            SQLX_OFFLINE = "true";
        };

        craneLibLLvmTools = craneLib.overrideToolchain
          (fenix.packages.${system}.complete.withComponents [
            "cargo"
            "llvm-tools"
            "rustc"
          ]);

        # Build *just* the cargo dependencies (of the entire workspace),
        # so we can reuse all of that work (e.g. via cachix) when running in CI
        # It is *highly* recommended to use something like cargo-hakari to avoid
        # cache misses when building individual top-level-crates
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # shallow merge -> //
        individualCrateArgs = commonArgs // {
          inherit cargoArtifacts;
          inherit (craneLib.crateNameFromCargoToml { inherit src; }) version;
          # NB: we disable tests since we'll run them all via cargo-nextest
          doCheck = false;
          SQLX_OFFLINE = "true";
        };

        fileSetForCrate = crate: lib.fileset.toSource {
          root = ./.;
          fileset = lib.fileset.unions [
            ./Cargo.toml
            ./Cargo.lock
            crate
          ];
        };

        # Build the top-level crates of the workspace as individual derivations.
        # This allows consumers to only depend on (and build) only what they need.
        # Though it is possible to build the entire workspace as a single derivation,
        # so this is left up to you on how to organize things
        my-cli = craneLib.buildPackage (individualCrateArgs // {
          pname = "iceberg-catalog-bin";
          cargoExtraArgs = "-p iceberg-catalog-bin";
          src = fileSetForCrate ./.;
            SQLX_OFFLINE = "true";

        });
        cli_cmd = pkgs.runCommand "iceberg-catalog" {} ''
          mkdir -p $out/bin
          cp ${my-cli}/bin/iceberg-catalog $out/bin
        '';
        dockerImage = pkgs.dockerTools.buildImage {
                            name = "iceberg-catalog";
                            tag = "latest";
                            copyToRoot = [ cli_cmd ];
                            config = {
#                              Entrypoint = [ "${my-cli}/bin/iceberg-catalog" ];
                            };
        };
      in
      {

        packages = {
          inherit my-cli dockerImage;

          default = my-cli;

        } // lib.optionalAttrs (!pkgs.stdenv.isDarwin) {

        };

        apps = {
          my-cli = flake-utils.lib.mkApp {
            drv = my-cli;
          };
        };

        devShells.default = craneLib.devShell {
          # Inherit inputs from checks.

          # Additional dev-shell environment variables can be set directly
          # MY_CUSTOM_DEVELOPMENT_VAR = "something else";

          # Extra inputs can be added here; cargo and rustc are provided by default.
          packages = [
            pkgs.cargo-hakari
          ];
        };
      });
}