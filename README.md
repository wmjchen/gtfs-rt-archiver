# GTFS-Realtime Archiver

`gtfs-rt-archiver` polls GTFS-Realtime endpoints, durably stores every bounded
HTTP 2xx body, compacts captures into immutable daily Parquet revisions, and
publishes those revisions to one or more rclone destinations.

The archiver is designed to run as a single Docker container with a persistent
`/data` volume. It deliberately keeps malformed, empty, or truncated 2xx responses:
their parse status is recorded and their original decoded bytes remain
available for later analysis.

Source definitions are data-driven via `config.example.yaml`, which bundles
eight Canadian feeds (TransLink in Vancouver with API-key auth, TTC in
Toronto, Hamilton Street Railway, BC Transit Victoria, Durham Region
Transit, Kingston Transit, Halifax Transit, and MiWay in Mississauga) as
runnable examples. Any GTFS-Realtime feed works; these can be replaced or
removed.

## Quick start

```sh
cp config.example.yaml config.yaml
# Edit destinations/attribution, then provide the referenced credentials.
# TRANSLINK_API_KEY is required only for the bundled TransLink example source;
# the other seven example feeds are open and need no credentials.
export TRANSLINK_API_KEY=...
docker build -t gtfs-rt-archiver .
docker run --rm \
  -p 8080:8080 \
  -e TRANSLINK_API_KEY \
  -v "$PWD/data:/data" \
  -v "$PWD/config.yaml:/config/config.yaml:ro" \
  -v "$PWD/rclone.conf:/config/rclone.conf:ro" \
  -v "$PWD/translink-attribution.txt:/config/translink-attribution.txt:ro" \
  gtfs-rt-archiver validate-config --config /config/config.yaml
```

Run continuously with `run` instead of `validate-config`. The container must
have exactly one replica for each data volume. In Coolify, use stop-first
deployments and mount `/data` as persistent storage. Mount an `rclone.conf`
read-only and point `rclone.config_file` at it when remote publication is
enabled.

## Commands

```text
gtfs-rt-archiver run --config FILE
gtfs-rt-archiver validate-config --config FILE
gtfs-rt-archiver fetch-once --config FILE [--source ID] [--stream ID]
gtfs-rt-archiver compact --config FILE --date YYYY-MM-DD [--source ID] [--stream ID]
gtfs-rt-archiver upload --config FILE [--destination ID]
gtfs-rt-archiver verify --config FILE --date YYYY-MM-DD [--source ID] [--stream ID]
gtfs-rt-archiver status --config FILE [--json]
gtfs-rt-archiver repair reconcile --config FILE
gtfs-rt-archiver retire-destination --config FILE --destination ID --reason TEXT
```

See `config.example.yaml` for the configuration contract. Feed credentials are
loaded only through environment variables or mounted files.

## Testing

The automated suite never contacts a live transit agency. It uses generated
vehicle-position, trip-update, alert, deleted-entity, differential, malformed,
and unknown-field protobuf fixtures behind in-memory HTTP transports. Run it with
memory-conservative settings via `make test`; a credentialed TransLink smoke
test remains an explicit operator step.
