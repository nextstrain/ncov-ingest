#! /usr/bin/env bash
set -euo pipefail
echo "Adding touchfiles to trigger full nextclade rerun"
set -x
aws s3 cp - s3://nextstrain-ncov-private/nextclade.tsv.zst.renew < /dev/null
aws s3 cp - s3://nextstrain-data/files/ncov/open/nextclade.tsv.zst.renew < /dev/null
set +x
echo "Done"