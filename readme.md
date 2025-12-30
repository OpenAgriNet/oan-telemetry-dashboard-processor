# Telemetry Log Processor

A NodeJS microservice that processes telemetry logs from a PostgreSQL database, extracts configurable event types, and stores them in appropriate tables.

## Features

- Processes telemetry logs from the `winston_logs` table in PostgreSQL
- Supports **dynamically configurable event types** through API or database
- Default processors for `OE_ITEM_RESPONSE` and `Feedback` events
- Auto-creates tables if they don't exist
- Updates the `sync_status` to 1 after processing a log
- Runs on a configurable schedule (customizable via environment variables)
- Processes a configurable batch size per run
- Provides API endpoints for manual processing, health checks, and event processor management
- Docker Compose setup for both development and production environments

## Prerequisites

- Node.js (v14+)
- PostgreSQL database
- Docker and Docker Compose (optional, for containerized deployment)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/telemetry-log-processor.git
   cd telemetry-log-processor
   ```

2. Install dependencies:
   ```
   npm install
   ```

3. Create a `.env` file from the example:
   ```
   cp .env.example .env
   ```

4. Update the `.env` file with your database credentials and other configuration options.

## Configuration

Edit the `.env` file to configure the service:

```
# Server Configuration
PORT=3000                # Service port
LOG_LEVEL=info           # Logging level
NODE_ENV=production      # Environment (production/development)

# Database Configuration
DB_USER=postgres         # Database user
DB_PASSWORD=postgres     # Database password
DB_HOST=localhost        # Database host
DB_PORT=5432             # Database port
DB_NAME=telemetry        # Database name

# Telemetry Processing Configuration
BATCH_SIZE=10            # Number of logs to process per run
CRON_SCHEDULE=*/5 * * * * # Cron schedule for processing
```

## Running the Service

### Development Mode

```
npm run dev
```

### Production Mode

```
npm start
```

### Using Docker Compose

#### Development Environment
```
docker-compose up telemetry-processor-dev
```

#### Production Environment
```
docker-compose up telemetry-processor
```

## API Endpoints

- **POST /api/process-logs**: Manually trigger log processing
- **GET /api/event-processors**: Get list of registered event processors
- **POST /api/event-processors**: Register a new event processor
- **GET /health**: Check service health

### Registering a New Event Processor

You can dynamically register new event processors using the API:

```bash
curl -X POST http://localhost:3000/api/event-processors \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "NEW_EVENT_TYPE",
    "tableName": "new_events",
    "fieldMappings": {
      "uid": "uid",
      "sid": "sid",
      "channel": "channel",
      "eventData": "edata.eks.data"
    }
  }'
```

## Database Schema

### winston_logs Table (Source)

```sql
CREATE TABLE IF NOT EXISTS public.winston_logs (
  level character varying COLLATE pg_catalog."default",
  message character varying COLLATE pg_catalog."default",
  meta json,
  "timestamp" timestamp without time zone DEFAULT now(),
  sync_status integer DEFAULT 0
)
```

### event_processors Table (Configuration)

```sql
CREATE TABLE IF NOT EXISTS public.event_processors (
  id SERIAL PRIMARY KEY,
  event_type VARCHAR NOT NULL UNIQUE,
  table_name VARCHAR NOT NULL,
  field_mappings JSONB NOT NULL,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
)
```

### Target Tables (Dynamically Created)

Target tables are created automatically based on the event processor configurations. By default, the system creates:

- **questions** table for `OE_ITEM_RESPONSE` events
- **feedback** table for `Feedback` events

## Testing

Run tests with:

```
npm test
```

This will run the Jest test suite with coverage reporting.

## Project Structure

```
telemetry-log-processor/
├── __tests__/               # Unit and API tests
├── .env.example             # Environment variables template
├── .env.dev                 # Development environment variables
├── Dockerfile               # Production Docker configuration
├── Dockerfile.dev           # Development Docker configuration
├── docker-compose.yml       # Docker Compose configuration
├── index.js                 # Main application file
├── eventProcessors.js       # Event processor management
├── logger.js                # Winston logger configuration
├── package.json             # Node.js dependencies and scripts
└── README.md                # Documentation
```

## Logging

Logs are written to:
- Console (with colorization)
- `error.log` (errors only)
- `combined.log` (all logs)

## License

MIT
# oan-telemetry-dashboard-processor
