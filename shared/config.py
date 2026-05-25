import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    POSTGRES_URL: str = os.getenv("DATABASE_URL", "postgresql://trajectory_user:trajectory_password@localhost:5432/trajectorylens")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    NEO4J_URL: str = os.getenv("NEO4J_URL", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "trajectory_password")
    QDRANT_URL: str = os.getenv("QDRANT_URL", "http://localhost:6333")
    
    INGESTION_STREAM_NAME: str = "trajectory_events"

settings = Settings()
