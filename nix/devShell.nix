{ darwin
, lib
, mkShell
, pkg-config
, python312
, stdenv
}:

# Note: [Python packages with C++ extensions]
#
# ref: <https://discourse.nixos.org/t/nix-shells-and-python-packages-with-c-extensions/26326>

let
  python-env = python312.withPackages (pp: [
    pp.certifi
    pp.urllib3
    pp.pandas
    pp.pandas-stubs
  ]);
in mkShell {
  buildInputs = [
    python-env
  ];

  packages = [
    python-env
  ];

  # See note: [Python packages with C++ extensions]
  nativeBuildInputs = [
    pkg-config
  ] ++ lib.optionals stdenv.isDarwin [
    # Add any Apple framework libraries your package needs, e.g.
    # darwin.apple_sdk.frameworks.IOKit
  ];

  env = {
    NIX_PYTHONPATH = lib.strings.concatStringsSep ":" [
      "${python-env}/${python-env.sitePackages}"
    ];

    PYTHONPATH = lib.strings.concatStringsSep ":" [
      "${python-env}/${python-env.sitePackages}"
    ];
  };

  shellHook = ''
    if [[ ! -d .venv ]]; then
      echo "No virtual env found at ./.venv, creating a new virtual env linked to the Python site defined with Nix"
      ${python-env}/bin/python -m venv .venv
      cp ${builtins.toString ./sitecustomize.py} .venv/lib/python*/site-packages/
    fi

    source .venv/bin/activate

    echo "Nix development shell loaded."
  '';
}
