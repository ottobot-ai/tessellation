# Tessellation Justfile

# Shows help
default:
    @just --list --justfile {{ justfile() }}


# Make sure dependencies are installed before running any recipe
_check_deps:
	@bash docker/bin/install_dependencies.sh

# Main test command: recompile, setup docker environment, run all e2e tests (including metagraph and multi-metagraph). Use --test=<name> to run specific tests, --list-tests to see available tests, --skip-assembly to reuse JARs.
test *extra_args:
	@just _check_deps
	@bash docker/bin/compose-runner.sh --use-test-metagraph --num-gl0=3 --metagraphs=2 {{ extra_args }}

# Bring up the default test environment, starting docker images but without running any tests or checks
# Use --hypergraph-release=<tag> to use pre-built JARs from a release (e.g., --hypergraph-release=v3.5.11)
# This is useful for metagraph development against a stable tessellation version
up *extra_args:
	@just _check_deps
	@bash docker/bin/compose-runner.sh --up {{ extra_args }}

# Destroy test environment, alias for clean-docker
down *extra_args:
	@just clean-docker

# Build the docker images and test environment, without running any containers
build *extra_args:
	@just _check_deps
	@bash docker/bin/compose-runner.sh --build {{ extra_args }}

purge-docker:
	@bash docker/bin/purge-docker.sh

clean-docker:
	@bash docker/bin/tessellation-docker-cleanup.sh

# Remove root-owned node data and logs for all layer types using a Docker container to bypass sudo
# Covers: gl0, gl1, ml0, cl1, dl1 — both data and logs directories
# Data lives in nodes/ (repo root, used by compose-runner) AND docker/nodes/ (legacy)
clean-data:
	@docker run --rm -v $(pwd)/nodes:/nodes alpine sh -c "\
	  for layer in gl0 gl1 ml0 cl1 dl1; do \
	    rm -rf /nodes/*/\$layer-data /nodes/*/\$layer-logs; \
	  done" 2>/dev/null || true
	@docker run --rm -v $(pwd)/docker/nodes:/nodes alpine sh -c "\
	  for layer in gl0 gl1 ml0 cl1 dl1; do \
	    rm -rf /nodes/*/\$layer-data /nodes/*/\$layer-logs; \
	  done" 2>/dev/null || true
	@echo "Node data and logs cleaned for gl0/gl1/ml0/cl1/dl1 (nodes/ and docker/nodes/)"

clean-configs:
	@bash docker/bin/clean-configs.sh

clean:
	@bash sbt clean
	@bash -c "cd .github/templates/metagraphs/project_template && sbt clean"
	@just clean-configs
	@just clean-docker

# NUKE EVERYTHING: docker state, all node data/logs/configs, all JAR caches,
# metagraph template build artifacts, and SBT target dirs. Use when you suspect
# stale state is masking code changes. After this, the next `just test` does
# a full cold rebuild (~5-10 min).
nuke:
	@echo "🔥 Nuking all state..."
	@just clean-docker
	@just clean-data
	@just clean-configs
	@echo "🔥 Removing deploy JAR cache (docker/jars/)..."
	@rm -f docker/jars/*.jar
	@echo "🔥 Removing metagraph template build artifacts..."
	@rm -rf .github/templates/metagraphs/project_template/modules/*/target
	@rm -rf .github/templates/metagraphs/project_template/project/target
	@rm -rf .github/templates/metagraphs/project_template/project/project
	@rm -rf .github/templates/metagraphs/project_template/target
	@echo "🔥 Running sbt clean (tessellation target dirs)..."
	@bash sbt clean
	@echo "🔥 All nuked. Next 'just test' will do a full cold rebuild."

debug-main:
	@just _check_deps
	@bash docker/bin/debug/mn-replicate.sh

check:
    @bash sbt --error 'scalafixAll --check --rules OrganizeImports;scalafmtCheckAll;test'

# Generate test user keys for bulk transaction testing
generate-test-keys num_keys='10':
	@bash docker/bin/generate-test-user-keys.sh {{ num_keys }}
