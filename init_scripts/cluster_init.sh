#!/bin/bash
# Cluster init script — TEACHING EXHIBIT.
#
# Free Edition is serverless-only and cannot run init scripts (they are a
# classic-compute feature). This file exists so you can see what one looks
# like; on a paid workspace it's wired up via the commented `job_clusters`
# block in resources/retail_pipeline.job.yml.
#
# Why init scripts exist: classic clusters boot as CLEAN machines every time.
# Anything your job needs beyond the Databricks runtime — pinned Python
# packages, dbt, private-repo credentials — must be installed at startup, and
# this script is where that happens. Pinning versions HERE is why the job
# behaves the same on every run: "works on my cluster" becomes reproducible.
#
# The serverless replacement for this file is the `environments:` block in
# the job YAML — same idea (declare dependencies, get a clean reproducible
# runtime), no shell script or cluster to manage.

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pip install -r "$SCRIPT_DIR/requirements.txt"
