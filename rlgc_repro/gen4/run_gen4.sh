#!/bin/bash
# gen4 app-shaped Ractor suite runner.
#
#   rlgc_repro/gen4/run_gen4.sh [RUBY] [timeout_sec] [filter]
#
#   RUBY        : ruby binary to test (default: ./ruby at repo root)
#   timeout_sec : per-file timeout (default: 60)
#   filter      : optional shell glob on basenames, e.g. 'sv_*'
#
# Every file must exit 0 AND print a line starting with "OK ".
# Exit status: number of failing files (0 = green).
set -u

dir=$(cd "$(dirname "$0")" && pwd)
srcdir=$(cd "$dir/../.." && pwd)
RUBY=${1:-$srcdir/ruby}
TMO=${2:-60}
FILTER=${3:-*}

fails=0
for f in "$dir"/$FILTER.rb; do
  [ -e "$f" ] || continue
  base=$(basename "$f" .rb)
  out=$(timeout "$TMO" "$RUBY" -W:no-experimental "$f" 2>&1)
  st=$?
  if [ $st -ne 0 ] || ! grep -q '^OK ' <<<"$out"; then
    fails=$((fails + 1))
    echo "FAIL($st) $base"
    grep -v "RubyGems" <<<"$out" | head -12 | sed 's/^/    /'
  else
    echo "ok   $base"
  fi
done
echo "gen4: $fails failure(s)"
exit $fails
