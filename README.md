# deephaven-zio-client

Scala / ZIO client experiments for Deephaven.

## Local integration test (Docker + `TestClient`)

This repo includes a simple integration-style publisher (`TestClient`) that:

- connects to a local Deephaven server on `localhost:10000`
- streams a small, append-only `ticks` table into Deephaven

### 1) Start Deephaven via Docker

From the Deephaven docs, a good default is to run with a PSK (password) and a persisted volume:

```bash
docker run --rm --name deephaven \
  -p 10000:10000 \
  -v data:/data \
  --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE \
  ghcr.io/deephaven/server:latest
```

Then open the Deephaven IDE:

- http://localhost:10000/ide/

You’ll be prompted for the password (`YOUR_PASSWORD_HERE`).

### 2) Run the local publisher (`TestClient`)

In a separate terminal:

```bash
export DEEPHAVEN_PSK='YOUR_PASSWORD_HERE'

# Required for Apache Arrow on Java 17+
export JAVA_TOOL_OPTIONS='--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED'

cd ~/code/deephaven-zio-client
sbt "runMain deephaven.zio.client.TestClient"
```

What you should see:

- the app logs “Connecting to Deephaven at localhost:10000”
- in the Deephaven IDE, a table named **`ticks`** appears and updates live

### 3) Stop

- Stop the Scala process with `Ctrl+C`.
- Stop Deephaven with `Ctrl+C` in the Docker terminal (since it’s running with `--rm`).

### Notes / Troubleshooting

- If you see `DEEPHAVEN_PSK is not set`, you forgot to export the env var.
- If you see an Arrow error about `MemoryUtil` / `--add-opens`, ensure `JAVA_TOOL_OPTIONS` is set.
- `TestClient` publishes to the table name `ticks` (see `TestClient.scala`).
