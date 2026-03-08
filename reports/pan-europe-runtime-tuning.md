# Pan-Europe Runtime Export Tuning (This Machine)

## Host Profile

- CPU: Intel i5-10600K (`12` logical CPUs, `6` cores / `2` threads)
- RAM: `31GiB` total
- Swap: `8GiB` (zram)
- Disk: WD_BLACK SN7100 1TB NVMe (`/home` on `nvme0n1p3`)
- PostGIS container hard cap: `20GiB` (`mem_limit=20g`, `memswap_limit=20g`)

## Monitoring During Run

- Export progress:

```bash
latest=$(ls -1dt /tmp/export-gtfs-batch-* | head -n 1)
done=$(($(wc -l < "$latest/trips.txt")-1))
total=$(docker exec -i trainscanner-postgis psql -U trainscanner -d trainscanner -Atc \
  "SELECT COUNT(*) FROM timetable_trips WHERE source_id='de_delfi_sollfahrplandaten_netex' AND (trip_start_date IS NULL OR trip_start_date <= DATE '2026-03-04') AND (trip_end_date IS NULL OR trip_end_date >= DATE '2026-03-04');")
awk -v d="$done" -v t="$total" 'BEGIN{printf("progress=%.2f%% (%d/%d)\n",(d*100.0)/t,d,t)}'
```

- Container memory:

```bash
docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' | rg 'NAME|trainscanner-postgis'
```

## Operational Notes

- Close heavy local apps during export (games/browsers) to reduce IO and CPU contention.
- If host enters power saving/suspend, resume may interrupt download/ingest/export jobs.
- If run fails mid-way, restart with the same settings; no schema rollback is required.

## Final Recommendation

- Use the `100000` trip batch cap for full mixed-source (`DE+CH`) runtime builds on this host.
- Use `parallel-gather-workers=6` with the current PG options.
- Do not use `500000+` for routine mixed-source local runs; the DE-only benchmark looked good, but the full two-source export OOM-killed the Python exporter at roughly `18.5GB` RSS.

## 2026-03-06 High-Throughput Sweep (DE-Only Benchmark)

- Environment changes applied:
  - PostGIS shared memory increased in compose: `shm_size: 1g`.
  - Table planner hints set:
    - `ALTER TABLE timetable_trips SET (parallel_workers = 4);`
    - `ALTER TABLE timetable_trip_stop_times SET (parallel_workers = 8);`
    - `ALTER TABLE provider_global_stop_point_mappings SET (parallel_workers = 4);`
  - Exporter now emits periodic progress logs and includes safe fallback for parallel shared-memory query failures.
  - Exporter ZIP writer now forces ZIP64 for very large GTFS files.

### Benchmark Command Pattern

```bash
bash scripts/qa/build-profile.sh \
  --profile pan_europe_runtime \
  --as-of 2026-03-04 \
  --tier all \
  --source-id de_delfi_sollfahrplandaten_netex \
  --query-mode optimized \
  --benchmark-max-batches 1 \
  --parallel-gather-workers 6 \
  --progress-interval-sec 10 \
  --pgoptions '-c work_mem=64MB -c maintenance_work_mem=256MB -c temp_buffers=32MB -c max_parallel_workers=12 -c parallel_setup_cost=0 -c parallel_tuple_cost=0 -c min_parallel_table_scan_size=0 -c min_parallel_index_scan_size=0'
```

### Batch Sweep Results (DE, 1 batch each)

- `40000`: runtime `67.85s`, DB fetch `55.73s`, `24072.967 trips/min`
- `120000`: runtime `80.79s`, DB fetch `64.35s`, `60665.021 trips/min`
- `200000`: runtime `90.39s`, DB fetch `69.23s`, `90354.436 trips/min`
- `400000`: runtime `129.34s`, DB fetch `90.45s`, `126104.155 trips/min`
- `500000`: runtime `128.49s`, DB fetch `90.72s`, `158595.135 trips/min`
- `600000`: runtime `148.12s`, DB fetch `106.18s`, `165092.834 trips/min`
- `800000`: runtime `169.72s`, DB fetch `116.88s`, `192072.621 trips/min`
- `1000000`: intentionally aborted by operator due host RAM/swap pressure during preparation window.

### Benchmark Interpretation

- These results were gathered against the DE source only.
- They were useful for query-shape tuning and planner settings.
- They were not sufficient to prove end-to-end memory safety for the final `DE+CH` runtime export.

### DE-Only Benchmark Command (Historical)

```bash
bash scripts/qa/build-profile.sh \
  --profile pan_europe_runtime \
  --as-of 2026-03-04 \
  --tier all \
  --batch-size-trips 500000 \
  --query-mode optimized \
  --parallel-gather-workers 6 \
  --progress-interval-sec 20 \
  --pgoptions '-c work_mem=64MB -c maintenance_work_mem=256MB -c temp_buffers=32MB -c max_parallel_workers=12 -c parallel_setup_cost=0 -c parallel_tuple_cost=0 -c min_parallel_table_scan_size=0 -c min_parallel_index_scan_size=0'
```

### DE-Only Full Result With Historical Settings

- Command used: same as above plus `--force`.
- Runtime: `1055.94s` (`~17.6 min`)
- Trips: `2337963`
- Stop times: `36317833`
- Throughput: `132845.786 trips/min`
- Batches: `7` on active DE source (`CH` source produced no exportable rows in current scope)

## 2026-03-08 Mixed-Source Stability Findings

- Full two-source export (`DE+CH`) with `batch-size-trips=500000` was not safe on this host.
- Kernel OOM evidence:
  - `python3` exporter process killed on `2026-03-08 15:01:59`
  - `anon-rss: 18,522,312kB`
- Practical implication:
  - the exporter, not Postgres, was the dominant memory consumer in the failing run.
  - DE-only benchmark ceilings cannot be reused blindly once the CH timetable batches are included.

### Final Stable Full Export Command (This Machine)

```bash
bash scripts/qa/build-profile.sh \
  --profile pan_europe_runtime \
  --as-of 2026-03-04 \
  --tier all \
  --batch-size-trips 100000 \
  --query-mode optimized \
  --parallel-gather-workers 6 \
  --progress-interval-sec 20 \
  --force \
  --pgoptions '-c work_mem=64MB -c maintenance_work_mem=256MB -c temp_buffers=32MB -c max_parallel_workers=12 -c parallel_setup_cost=0 -c parallel_tuple_cost=0 -c min_parallel_table_scan_size=0 -c min_parallel_index_scan_size=0'
```

### Final Stable Full Export Result

- Artifact:
  - `data/gtfs/runtime/pan_europe_runtime/2026-03-04/active-gtfs.zip`
- SHA256:
  - `38924cc73434b050bcaa9f6f4a4a8b89a8b3c2007ce72a2a34999e58cda8491c`
- Runtime:
  - `5287563.721 ms` (`~88.1 min`)
- Counts:
  - `trips=3451379`
  - `stopTimes=54315223`
  - `stops=647825`
  - `transfers=996514`
  - `countries=2`
- Batching/performance:
  - `tripBatches=47`
  - `sourcesProcessed=2`
  - `fetchMsP50=102307.958`
  - `fetchMsP95=144287.33`
  - `rowsP50=1063557`
  - `rowsP95=1619459`
  - `tripsPerMinute=39164.112`

### Working Memory Envelope

- Stable observed values during the final `100000` run:
  - exporter Python RSS: about `2.95GiB`
  - PostGIS container: about `1.65GiB / 20GiB`
- Recommendation:
  - `100000` is the default safe full-run cap for this desktop while keeping the browser/IDE open.
  - `200000` may be acceptable in a cleaner session, but it is not the documented default because it was not the final validated setting for the full Phase 5 closure run.

### K8s Prep Memory Note (2026-03-08)

- `scripts/qa/prepare-motis-k8s-artifacts.py` originally blew up host RAM on `micro --tier all` because it materialized large GTFS tables in Python.
- The script now has a streaming low-memory micro fast path.
- Verified prep memory after the fix:
  - max RSS `130228 kB` (`~127 MiB`)
- Operational consequence:
  - local k8s preparation is no longer a host-RAM bottleneck for Phase 5 validation.

### K8s MOTIS Memory Note (2026-03-08)

- `motis-runner` previously OOM-killed with a `6Gi` container limit.
- The k8s templates now use:
  - requests: `4Gi`
  - limits: `12Gi`
- This setting was validated by successful March 8 `micro` and `macro` test runs.

### Macro Test Memory Note (2026-03-06, still relevant)

- `scripts/qa/prepare-motis-k8s-artifacts.py` was updated with a macro `tier=all` fast path:
  - reads only calendar tables for query-date selection,
  - streams active-trip query selection from `trips.txt` + `stop_times.txt`,
  - copies GTFS ZIP directly instead of full in-memory table materialization/rewrite.
- Outcome:
  - macro prep no longer drives host memory into near-OOM territory.
  - macro validation now passes using deterministic in-feed tagged stop queries.
