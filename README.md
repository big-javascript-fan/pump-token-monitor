# token-monitor

Rust script to track Pump.fun token mints via:

- **`backfill`** (default): Helius JSON-RPC — `getSignaturesForAddress` on the Pump program, then `getTransaction` per signature; detects `create` / `create_v2`.
- **`enhanced`**: Helius Enhanced REST API (`/v0/addresses/.../transactions`) for historical paging (optional).
- **`logs`**: WebSocket `logsSubscribe` (program mentions).

New mints are stored in Postgres (when configured) and exposed via the HTTP API; an optional `import_token_json` binary can seed from a JSON file.

## Run

1. Edit `config.toml` (required).
2. From this folder:

```bash
cargo run --release
```

## Modes (`stream_mode`)

| Mode | Behavior |
| --- | --- |
| `backfill` | Historical scan over **default Helius RPC** (POST JSON-RPC), not Enhanced REST |
| `enhanced` | Historical scan via **Helius Enhanced API** (`helius_enhanced_base_url`) |
| `logs` | Live stream only |
| `both` | Runs `logs` and **`backfill` (RPC)** in parallel (`tokio::select!` — first completion ends the process) |

Default `stream_mode` if omitted: `backfill`.

## `config.toml`

| Field | Purpose |
| --- | --- |
| `helius_api_key` | Needed to build RPC URL unless `rpc_url` is set. Required for `enhanced`. |
| `rpc_url` | Full HTTPS RPC URL if you do not want the default `mainnet.helius-rpc.com` + key URL |
| `pump_program_id` | Defaults to mainnet Pump.fun program |
| `stream_mode` | `backfill` \| `enhanced` \| `logs` \| `both` |
| `helius_enhanced_base_url` | Host for Enhanced API only (`enhanced` mode); default `https://api.helius.xyz` |
| `max_signatures` | Optional cap on signatures processed in **RPC backfill** (default ~500k) |
| `tx_request_delay_ms` | Delay between `getTransaction` calls in RPC backfill (default 40) |
| `signatures_page_size` | `getSignaturesForAddress` page size 1–1000 (default 1000) |

RPC URL resolution: **`rpc_url`** if set; else **`https://mainnet.helius-rpc.com/?api-key=`** + **`helius_api_key`**.

Settings are read **only** from `config.toml`.
