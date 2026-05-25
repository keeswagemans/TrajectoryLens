# TrajectoryLens Backend

The TrajectoryLens backend is a distributed, event-driven system designed to ingest, process, and analyze agent cognitive trajectories in real-time.

## Architecture

The backend consists of several microservices and shared components:

- **Ingestion Service (`ingestion/`)**: A FastAPI-based service that receives events and run data from the SDK. It validates requests, persists basic data to PostgreSQL, and pushes events to a Redis Stream for asynchronous processing.
- **API Service (`api/`)**: The main gateway for the frontend. It provides endpoints for querying runs, events, and trajectory graphs. It bridges PostgreSQL (relational data), Neo4j (graph-based causal relations), and Qdrant (vectorized state similarity).
- **Processing Worker (`worker/`)**: A background consumer that processes events from the Redis Stream. It builds the causal graph in Neo4j, generates embeddings for state similarity in Qdrant, and performs hindsight credit assignment.
- **Shared (`shared/`)**: Common models, database configuration, and settings used across all services.

## Tech Stack

- **FastAPI**: High-performance web framework for the Ingestion and API services.
- **PostgreSQL**: Primary relational storage for projects, runs, and event metadata.
- **Neo4j**: Graph database used for representing and querying complex branching trajectories and causal relationships.
- **Qdrant**: Vector database for similarity search across agent cognitive states.
- **Redis**: Used as a message broker (Redis Streams) and for caching.
- **SQLAlchemy**: ORM for PostgreSQL.
- **Pydantic**: Data validation and settings management.

## Getting Started

### Prerequisites

- Python 3.13+
- `uv` (recommended)
- Docker and Docker Compose (to run the infrastructure)

### Setup

1.  **Start Infrastructure**:
    ```bash
    cd ../docker
    docker compose up -d
    ```

2.  **Install Dependencies**:
    ```bash
    cd backend
    uv sync
    ```

3.  **Run Services**:

    **Ingestion Service**:
    ```bash
    uv run uvicorn ingestion.main:app --port 8000 --reload
    ```

    **API Service**:
    ```bash
    uv run uvicorn api.main:app --port 8001 --reload
    ```

    **Processing Worker**:
    ```bash
    uv run python -m worker.main
    ```

## API Documentation

Once the services are running, you can access the Swagger UI:
- Ingestion: `http://localhost:8000/docs`
- API: `http://localhost:8001/docs`
