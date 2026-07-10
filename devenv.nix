{
  pkgs,
  config,
  ...
}:

let
  user = "test";
  password = "test";
  db = "test";
in
{
  # Packages
  packages = with pkgs; [
    go
    gotools
    gopls
    govulncheck
    golangci-lint
    gofumpt
    gcc
    git
    sqlc
    sqlfluff
    postgresql_17
    treefmt
  ];

  # Cachix
  cachix.enable = false;

  # Go
  env.GOFUMPT_SPLIT_LONG_LINES = "on";
  languages.go = {
    enable = true;
    package = pkgs.go;
  };

  # Postgres
  env.TEST_DATABASE_URL = "postgres://${user}:${password}@localhost:${toString config.services.postgres.port}/${db}";
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    port = 6543;
    listen_addresses = "localhost";

    settings = {
      fsync = "off";
      full_page_writes = "off";
      synchronous_commit = "off";
      log_statement = "all";
      shared_buffers = "128MB";
      max_connections = "10000";
    };

    initialScript = ''
      CREATE USER ${user} SUPERUSER PASSWORD '${password}';
      CREATE DATABASE ${db} OWNER ${user};
    '';
  };

  treefmt = {
    enable = true;
    config.programs = {
      nixfmt.enable = true;
      gofumpt.enable = true;
      yamlfmt.enable = true;
      typos.enable = true;
    };
  };

  tasks = {
    "go:mod" = {
      exec = ''
        go mod tidy
        go mod download
      '';
      description = "Tidy and download Go modules";
    };

    "go:gen" = {
      exec = ''
        go mod tidy
        cd internal/queries
        sqlc generate
      '';
      description = "Run code generation";
    };

    "go:lint" = {
      exec = ''
        golangci-lint run
        sqlfluff lint --ignore parsing
      '';
      description = "Run Go and SQL linters";
    };

    "go:lint-fix" = {
      exec = ''
        golangci-lint run --fix
        sqlfluff fix --ignore parsing
        treefmt
      '';
      description = "Fix lint issues and format";
    };

    "go:test" = {
      exec = "go test -v -race -count=1 ./... -timeout 30m";
      description = "Run Go tests";
    };

    "go:test-ci" = {
      exec = "go test -count=1 ./... -timeout 30m";
      description = "Run CI tests";
    };

    "go:test-acceptance" = {
      exec = "go test -v -count=1 ./internal/migration_acceptance_tests/... -timeout 30m";
      description = "Run migration acceptance tests";
    };

    "sql:gen" = {
      exec = ''
        cd internal/queries
        sqlc generate
      '';
      description = "Regenerate sqlc query code";
    };

    "sql:lint" = {
      exec = "sqlfluff lint --ignore parsing";
      description = "Run SQL lint";
    };

    "sql:lint-fix" = {
      exec = "sqlfluff fix --ignore parsing";
      description = "Fix SQL lint issues";
    };
  };

  # Git hooks
  git-hooks = {
    hooks = {
      treefmt = {
        enable = true;
        require_serial = true;
      };

      lint = {
        enable = true;
        name = "lint";
        description = "Go and SQL lint";
        entry = "devenv tasks run go:lint";
        types = [ "go" ];
        pass_filenames = false;
      };
    };
  };
}
