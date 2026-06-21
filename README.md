# EventLens — Server

API server and Kafka worker for [EventLens](https://github.com/ydv-ankit/eventlens-server) — an open-source, self-hostable analytics platform built for high-throughput event ingestion.

## Related Repositories

| Repo | Description |
|---|---|
| [eventlens-server](https://github.com/ydv-ankit/eventlens-server) | This repo — API, worker, and infrastructure |
| [eventlens-sdk](https://github.com/ydv-ankit/eventlens-sdk) | JavaScript browser SDK (`eventlens-js` on npm) |

## Architecture

Events flow through the following pipeline:

```
Client SDK → NGINX (:8080) → API Server → Kafka → Worker → PostgreSQL
                                                      ↓
                                               Redis (real-time counters)
```

- **API Server** — Express app that validates API keys, publishes events to Kafka, and serves analytics queries
- **Worker** — Kafka consumer that batch-inserts events into PostgreSQL and updates Redis counters
- **Observability** — OpenTelemetry tracing (Jaeger), Prometheus metrics, Grafana dashboards

## Tech Stack

- **Node.js** + TypeScript + Express
- **Kafka** — event ingestion queue (4 partitions)
- **PostgreSQL** — event storage (`analytics.raw_event` table)
- **Redis** — real-time counters
- **Clerk** — authentication
- **OpenTelemetry** — distributed tracing
- **Prometheus** + **Grafana** — metrics and dashboards
- **Jaeger** — trace visualisation
- **NGINX** — reverse proxy
- **Docker Compose** — local stack

## Getting Started

### Prerequisites

- Docker + Docker Compose
- Node.js 20+

### 1. Clone and install

```bash
git clone https://github.com/ydv-ankit/eventlens-server
cd eventlens-server
npm install
```

### 2. Configure environment

```bash
cp env.example .env
```

Edit `.env` — at minimum set your Clerk keys:

```env
CLERK_SECRET_KEY=sk_...
CLERK_PUBLISHABLE_KEY=pk_...
```

### 3. Start the infrastructure

```bash
docker compose up -d
```

### 4. Run the API server and worker

```bash
# In two separate terminals
npm run dev:server
npm run dev:worker
```

The API is available at `http://localhost:8080`.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `APP_PORT` | `8080` | API server port |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `postgres` | PostgreSQL user |
| `DB_PASSWORD` | `postgres` | PostgreSQL password |
| `DB_NAME` | `eventlens` | PostgreSQL database name |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `eventlens-events` | Main ingestion topic |
| `KAFKA_TOPIC_PARTITIONS` | `4` | Number of topic partitions |
| `CLERK_SECRET_KEY` | — | Clerk secret key (required) |
| `CLERK_PUBLISHABLE_KEY` | — | Clerk publishable key (required) |
| `CORS_ORIGINS` | `http://localhost:5173` | Comma-separated allowed origins |
| `OTLP_TRACE_EXPORTER` | `http://jaeger:4318` | OTLP trace endpoint |

## API Endpoints

### Event Ingestion (public)

```
POST /event
```

```json
{
  "api_key":    "el_your_api_key",
  "event_name": "page_view",
  "user_id":    "user_123",
  "session_id": "9374f388-...",
  "timestamp":  "2026-06-21T10:00:00.000Z",
  "metadata":   { "path": "/dashboard" }
}
```

### Analytics (authenticated)

```
GET /analytics/overview
GET /analytics/events/volume?project_id=&interval=1h|24h|7d|30d
GET /analytics/events/top?project_id=&limit=
GET /analytics/events/recent?project_id=&limit=
```

### Other

```
GET  /healthz          — liveness probe
GET  /readyz           — readiness probe (checks Kafka)
GET  /metrics          — Prometheus metrics scrape endpoint
GET  /project          — project management
GET  /event            — event query (authenticated)
GET  /users/:id/events — user event timeline
GET  /system/health    — system health status
```

## Services and Ports

| Service | Port | Description |
|---|---|---|
| NGINX | `8080` | Reverse proxy → API server |
| PostgreSQL | `5432` | Primary database |
| Redis | `6379` | Real-time counters |
| Kafka | `9092` | Event queue |
| Kafka UI | `8085` | Kafka topic browser |
| Grafana | `3000` | Metrics dashboards |
| Prometheus | `9090` | Metrics storage |
| Jaeger | `16686` | Distributed trace UI |

## npm Scripts

| Script | Description |
|---|---|
| `npm run dev:server` | Start API server with hot reload |
| `npm run dev:worker` | Start Kafka worker with hot reload |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm run start:server` | Run compiled API server |
| `npm run start:worker` | Run compiled worker |

## Kubernetes

A Helm chart is available in `helm/` for deploying to Kubernetes.

```bash
helm install eventlens ./helm
```

## License

MIT
