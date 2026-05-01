#!/usr/bin/env bash
set -euo pipefail

# Submit the Storm Flux topology from the nimbus container.
#
# Usage (from project root):
#   bash scripts/submit_storm_topology.sh

export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

echo "Submitting Storm topology via Flux..."
docker exec -i storm_nimbus bash -lc '
  set -euo pipefail
  ls -la /opt/storm_app/storm/flux/crime_topology.yaml >/dev/null
  FLUX_JAR=$(ls /apache-storm/extlib/flux-core-*.jar | head -n 1)
  storm jar "$FLUX_JAR" org.apache.storm.flux.Flux --remote /opt/storm_app/storm/flux/crime_topology.yaml
'

echo "Done. Open Storm UI at http://localhost:8080"

