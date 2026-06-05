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
	@# BLANKET delete via a ROOT docker container — node containers run as root and leave root-owned
	@# files (data/logs, and nodes/N/kes/kes-sk.bin) the host `euler` user cannot remove or overwrite.
	@# The old selective per-layer rm left root-owned residue that poisoned the next run's genesis setup
	@# (cp -> "Permission denied" at ./nodes/N/kes/kes-sk.bin). Blanket-remove everything incl. hidden.
	@docker run --rm -v $(pwd)/nodes:/nodes alpine sh -c "rm -rf /nodes/* /nodes/.[!.]* /nodes/..?* 2>/dev/null; true" 2>/dev/null || true
	@docker run --rm -v $(pwd)/docker/nodes:/nodes alpine sh -c "rm -rf /nodes/* /nodes/.[!.]* /nodes/..?* 2>/dev/null; true" 2>/dev/null || true
	@echo "Node data/logs/keys/kes cleaned (BLANKET, root-owned-safe via docker) for nodes/ and docker/nodes/"

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
	@echo "🔥 Removing published SDK the metagraph BUNDLES (~/.ivy2/local/.../tessellation-sdk_2.13)..."
	@rm -rf ~/.ivy2/local/io.constellationnetwork/tessellation-sdk_2.13
	@echo "🔥 Running sbt clean (tessellation target dirs)..."
	@bash sbt clean
	@echo "🔥 All nuked. Next 'just test' will do a full cold rebuild."

# NUKE just the METAGRAPH binaries — faster than a full `nuke` when ONLY upstream code changed.
# Blasts every place a metagraph binary hides: the deploy JARs (docker/jars/{ml0,cl1,dl1}.jar), the
# project_template assembly targets, AND the published tessellation-sdk in ~/.ivy2/local that the
# metagraph jars BUNDLE — the sneaky stale source: changes to dag-l1/node-shared/shared/sdk do NOT
# re-bundle into ml0/cl1/dl1 without this. Leaves the tessellation build intact, so the next
# `just test` republishes the SDK + re-bundles the metagraph incrementally (no full cold rebuild).
nuke-metagraph:
	@echo "🔥 Nuking METAGRAPH binaries only (tessellation build left intact)..."
	@echo "🔥   deploy JARs (docker/jars/{ml0,cl1,dl1}.jar)..."
	@rm -f docker/jars/ml0.jar docker/jars/cl1.jar docker/jars/dl1.jar
	@echo "🔥   template assembly targets (project_template)..."
	@rm -rf .github/templates/metagraphs/project_template/modules/*/target
	@rm -rf .github/templates/metagraphs/project_template/project/target
	@rm -rf .github/templates/metagraphs/project_template/project/project
	@rm -rf .github/templates/metagraphs/project_template/target
	@echo "🔥   published SDK the metagraph BUNDLES (~/.ivy2/local/.../tessellation-sdk_2.13)..."
	@rm -rf ~/.ivy2/local/io.constellationnetwork/tessellation-sdk_2.13
	@echo "✅ Metagraph nuked. Next 'just test' republishes the SDK + re-bundles ml0/cl1/dl1 (no full cold rebuild)."

debug-main:
	@just _check_deps
	@bash docker/bin/debug/mn-replicate.sh

check:
    @bash sbt --error 'scalafixAll --check --rules OrganizeImports;scalafmtCheckAll;test'

# Generate test user keys for bulk transaction testing
generate-test-keys num_keys='10':
	@bash docker/bin/generate-test-user-keys.sh {{ num_keys }}
