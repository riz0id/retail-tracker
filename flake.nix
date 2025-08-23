{
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;

          overlays = [ (import nix/overlays.nix) ];
        };
      in {
        devShell = pkgs.callPackage ./nix/devShell.nix { };
      }
    );
}
