# LiveRamp Multi-Source Data Ingestion Pipeline

Production-grade data ingestion pipeline built with Apache Kafka and Spring Boot — mirrors LiveRamp's Ingestion team architecture.

## Architecture
```
CSV Files + REST APIs + Webhooks
           ↓
    Kafka (raw-records topic)
           ↓
  Validation Service (Spring Boot)
   ↓                    ↓
PASS                  FAIL
   ↓                    ↓
clean-records      dead-letter-queue
```

## Tech Stack
- Java 21 + Spring Boot 3.5
- Apache Kafka (Docker)
- GCP Pub/Sub + BigQuery + GCS
- Docker + Kubernetes (GKE)
- OpenTelemetry + Grafana

## Features
- Multi-source ingestion: CSV, REST API, Webhooks
- Schema validation (id, email required fields)
- PII anonymization using SHA-256 hashing
- Dead Letter Queue for failed records
- Retry logic up to 3 attempts

## How to Run
1. Start Kafka: `docker-compose up -d`
2. Start app: `mvnw spring-boot:run`
3. Test: POST to `http://localhost:8081/ingest/webhook`

## API Endpoints
- POST /ingest/file — Ingest CSV file data
- POST /ingest/api — Ingest REST API data
- POST /ingest/webhook — Ingest webhook data
- GET /ingest/health — Health check

## Sample Request
```json
{
  "id": "001",
  "name": "Naveen Karanam",
  "email": "naveen@test.com",
  "source": "WEBHOOK",
  "timestamp": "2026-03-15"
}
```
