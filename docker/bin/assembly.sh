
assemble_all() {
  sbt dagL0/assembly dagL1/assembly keytool/assembly wallet/assembly tools/assembly
}

# Fast path: skip all assembly when docker/jars/ is pre-populated (e.g., from CI artifact cache)
if [ "$SKIP_ASSEMBLY" = "true" ] && [ -f "$PROJECT_ROOT/docker/jars/gl0.jar" ] && [ -s "$PROJECT_ROOT/docker/jars/gl0.jar" ]; then
  if [ -z "$METAGRAPH" ] || { [ -f "$PROJECT_ROOT/docker/jars/ml0.jar" ] && [ -s "$PROJECT_ROOT/docker/jars/ml0.jar" ]; }; then
    echo "Pre-built JARs found in docker/jars/, skipping assembly"
    if [ -z "$METAGRAPH" ]; then
      touch "$PROJECT_ROOT/docker/jars/ml0.jar"
      touch "$PROJECT_ROOT/docker/jars/cl1.jar"
      touch "$PROJECT_ROOT/docker/jars/dl1.jar"
    fi
    show_time "Assembly (skipped - using pre-built JARs)"
    cd $PROJECT_ROOT
    return 0 2>/dev/null || true
  fi
fi

show_time "Starting assembly"

# If HYPERGRAPH_RELEASE is set, download pre-built JARs instead of building from source
if [ -n "$HYPERGRAPH_RELEASE" ]; then
  echo "Using pre-built hypergraph JARs from release: ${HYPERGRAPH_RELEASE}"
  rm -rf ./docker/jars/ > /dev/null 2>&1 || true
  mkdir -p ./docker/jars/
  
  ./docker/bin/download-release-jars.sh "$HYPERGRAPH_RELEASE" ./docker/jars/
  
  show_time "Downloaded release JARs"
  
  # Skip the rest of hypergraph assembly, jump to metagraph handling
  SKIP_HYPERGRAPH_BUILD=true
else
  SKIP_HYPERGRAPH_BUILD=false
fi

if [ "$SKIP_HYPERGRAPH_BUILD" != "true" ]; then
  # Build hypergraph JARs from source.
  #
  # SKIP_ASSEMBLY=false means "rebuild everything". The previous "only dagL0/dagL1"
  # default left tools.jar (genesis generator) + keytool/wallet stale whenever code
  # in those modules — or in any module they transitively depend on, like node-shared
  # — was edited. Symptom: edits to genesis types (e.g. `L0GenesisKesRegistration`
  # gains a field) compile cleanly via `sbt tools/compile` but the deployed tools.jar
  # still serializes the old schema, producing genesis JSON that the freshly-built
  # gl0.jar then fails to decode. See `project_tessellation_build` memory for the
  # general "stale JAR cache" failure mode.
  if [ "$SKIP_ASSEMBLY" == "false" ]; then
    if [[ "$INCLUDE_L0" == "true" && "$INCLUDE_L1" == "false" ]]; then
      echo "Assembling L0 only (--include-l1=false)"
      sbt dagL0/assembly
    elif [[ "$INCLUDE_L0" == "false" && "$INCLUDE_L1" == "true" ]]; then
      echo "Assembling L1 only (--include-l0=false)"
      sbt dagL1/assembly
    else
      echo "Assembling all modules (dagL0, dagL1, keytool, wallet, tools)"
      assemble_all
    fi
  else
    missing=false
    for module in dag-l0 dag-l1 keytool wallet tools; do
      set +e
      jar_path=$(ls -1t modules/"$module"/target/scala-2.13/tessellation-"$module"-assembly*.jar 2>/dev/null | head -n1)
      set -e
      if [ -z "$jar_path" ]; then
        echo "⚠️  Missing JAR for module: $module"
        missing=true
        break
      fi
    done

    if [ "$missing" = true ]; then
      echo "▶️  One or more modules is missing. Cannot skip assembly. Running full assembly"
      assemble_all
    else
      echo "Found existing assemblies, and skip assembly was set to true"
    fi
  fi

  show_time "SBT Assembly"

  rm -rf ./docker/jars/ > /dev/null 2>&1 || true;
  mkdir -p ./docker/jars/

  for module in "dag-l0" "dag-l1" "keytool" "wallet" "tools"
  do
    path=$(ls -1t modules/${module}/target/scala-2.13/tessellation-${module}-assembly*.jar | head -n1)
    dest="$PROJECT_ROOT/docker/jars/${module}.jar"
    cp "$path" "$dest"
  done

  mv ./docker/jars/dag-l0.jar ./docker/jars/gl0.jar
  mv ./docker/jars/dag-l1.jar ./docker/jars/gl1.jar
fi


if [ "$PUBLISH" == "true" ]; then
  if [ -n "$HYPERGRAPH_RELEASE" ]; then
    echo "Skipping sdk/publishLocal - using release ${HYPERGRAPH_RELEASE}"
    echo "  Note: Metagraph builds require tessellation-sdk ${HYPERGRAPH_RELEASE#v} from Maven Central."
    echo "  SNAPSHOT versions are not published - use a tagged release."
  else
    echo "Publishing local with version: $TESSELLATION_VERSION"
    sbt --error "set ThisBuild / version := \"$TESSELLATION_VERSION\"" sdk/publishLocal
  fi
fi


assemble_all_metagraph() {
  sbt --error currencyL0/assembly currencyL1/assembly dataL1/assembly
}



if [ -z "$METAGRAPH" ]; then
  touch ./docker/jars/ml0.jar
  touch ./docker/jars/cl1.jar
  touch ./docker/jars/dl1.jar
fi

move_metagraph_jar() {
  local module=$1
  local destination=$2
  path=$(ls -1t modules/${module}/target/scala-2.13/*-assembly*.jar | head -n1)
  dest="$PROJECT_ROOT/docker/jars/${destination}.jar"
  # Stale-JAR guard (the recurring metagraph cl1/dl1 Zinc gotcha): catch the case where a SOURCE file
  # changed but `sbt <module>/assembly` produced no fresh JAR — Zinc doesn't reliably invalidate a
  # metagraph subproject when only the upstream tessellation-sdk ivy version bumps, so the assembly can
  # be a silent no-op that `ls -1t | head` then deploys as a schema-mismatched node (decode failures vs
  # the new gl0 wire format). The correct signal is "JAR reflects current source", NOT "JAR newer than
  # this run" — Zinc legitimately SKIPS recompiling when nothing changed since the last build, and that
  # up-to-date reused JAR must pass. So compare the JAR's mtime against the NEWEST source file under the
  # module: stale (a source is newer than the JAR) -> fail loudly; up-to-date (JAR >= all sources, incl.
  # the Zinc-skipped-no-change case) -> deploy. Guard active only when assembly ran this invocation
  # (METAGRAPH_ASSEMBLY_MARKER set; the SKIP_METAGRAPH_ASSEMBLY=true path legitimately reuses JARs).
  if [ -n "${METAGRAPH_ASSEMBLY_MARKER:-}" ]; then
    if [ -z "$path" ]; then
      echo "❌ NO METAGRAPH JAR for module '$module' → '$destination': assembly produced nothing."
      echo "   Force a clean rebuild: rm -rf '$METAGRAPH'/modules/*/target (or 'just nuke')."
      exit 1
    fi
    # Newest .scala source under the module (build.sbt too); empty if none found.
    newest_src=$(find "modules/${module}/src" "modules/${module}/build.sbt" -type f \( -name '*.scala' -o -name 'build.sbt' \) 2>/dev/null \
      | xargs -r ls -1t 2>/dev/null | head -n1)
    if [ -n "$newest_src" ] && [ "$newest_src" -nt "$path" ]; then
      echo "❌ STALE METAGRAPH JAR for module '$module' → '$destination': source '$newest_src' is newer"
      echo "   than the assembled JAR '$path' — sbt assembly was a no-op (Zinc did not recompile a"
      echo "   changed source, likely an SDK version bump that didn't invalidate the subproject)."
      echo "   Deploying it would ship a schema-mismatched node. Force a clean rebuild:"
      echo "   rm -rf '$METAGRAPH'/modules/*/target (or 'just nuke'), then re-run."
      exit 1
    fi
  fi
  cp "$path" "$dest"
}


if [ -n "$METAGRAPH" ]; then
  echo "Assembling $METAGRAPH"
  cd $METAGRAPH

  # Stamp a marker just before assembly so move_metagraph_jar can assert each produced JAR is newer
  # (catches the Zinc-no-op stale-JAR trap). Only set when assembly will actually run — the
  # SKIP_METAGRAPH_ASSEMBLY=true path intentionally reuses existing JARs, so leave the marker unset
  # there to disable the freshness assertion.
  if [ "$SKIP_METAGRAPH_ASSEMBLY" != "true" ]; then
    export METAGRAPH_ASSEMBLY_MARKER="$(mktemp)"
  fi

  missing=false

  for module in $METAGRAPH_ML0_RELATIVE_PATH $METAGRAPH_CL1_RELATIVE_PATH $METAGRAPH_DL1_RELATIVE_PATH; do
    set +e
    jar_path=$(ls -1t modules/"$module"/target/scala-2.13/*-assembly*.jar 2>/dev/null | head -n1)
    set -e
    if [ -z "$jar_path" ]; then
      echo "⚠️  Missing JAR for module: $module"
      missing=true
      break
    fi
  done

  if [ "$missing" = true ]; then
    echo "▶️  One or more modules is missing. Cannot skip assembly. Running full assembly"
    assemble_all_metagraph
  else
    if [ "$SKIP_METAGRAPH_ASSEMBLY" == "false" ]; then
      override_set=false
      if [ "$METAGRAPH_ML0" == "true" ]; then
        echo "Assembling L0"
        echo "Assembling currencyL0 with explicit version: $TESSELLATION_VERSION"
        sbt --error currencyL0/assembly
        override_set=true
      fi
      if [ "$METAGRAPH_CL1" == "true" ]; then
        echo "Assembling CL1"
        echo "Assembling currencyL1 with explicit version: $TESSELLATION_VERSION"
        sbt --error currencyL1/assembly
        override_set=true
      fi
      if [ "$METAGRAPH_DL1" == "true" ]; then
        echo "Assembling DL1"
        echo "Assembling dataL1 with explicit version: $TESSELLATION_VERSION"
        sbt --error dataL1/assembly
        override_set=true
      fi
      if [ "$override_set" == "false" ]; then
        echo "Assembling all metagraph modules (default behavior)"
        echo "Assembling with explicit version: $TESSELLATION_VERSION"
        assemble_all_metagraph
      fi
    else
      echo "Found existing assemblies, and skip assembly was set to true"
    fi

  fi

  show_time "SBT Metagraph Assembly"


  move_metagraph_jar $METAGRAPH_ML0_RELATIVE_PATH "ml0"
  move_metagraph_jar $METAGRAPH_CL1_RELATIVE_PATH "cl1"
  move_metagraph_jar $METAGRAPH_DL1_RELATIVE_PATH "dl1"

fi

cd $PROJECT_ROOT