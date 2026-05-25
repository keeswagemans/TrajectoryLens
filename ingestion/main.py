from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import redis
import json
from datetime import datetime
from typing import Optional, Any, Dict, List
from ..shared.config import settings
from ..shared.database import get_db, engine
from ..shared import models

# Initialize FastAPI
app = FastAPI(title="TrajectoryLens Ingestion Service")

# Initialize Redis
r = redis.from_url(settings.REDIS_URL)

# Ensure tables exist (for development)
models.Base.metadata.create_all(bind=engine)

class RunCreate(BaseModel):
    run_id: str
    project_name: str
    agent_name: str
    task: str
    metadata: Dict[str, Any] = {}

class EventIngest(BaseModel):
    event_id: str
    run_id: str
    timestamp: datetime
    event_type: str
    name: str
    data: Dict[str, Any] = {}
    parent_id: Optional[str] = None
    tags: List[str] = []

@app.post("/runs")
async def create_run(run_data: RunCreate, db = Depends(get_db)):
    # 1. Ensure project exists
    project = db.query(models.Project).filter(models.Project.name == run_data.project_name).first()
    if not project:
        project = models.Project(name=run_data.project_name)
        db.add(project)
        db.commit()
        db.refresh(project)
    
    # 2. Create run in Postgres
    run = models.Run(
        id=run_data.run_id,
        project_id=project.id,
        agent_name=run_data.agent_name,
        task=run_data.task,
        metadata_json=run_data.metadata
    )
    db.add(run)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        # Might already exist
        pass
    
    # 3. Notify processing workers via Redis
    r.xadd(settings.INGESTION_STREAM_NAME, {
        "type": "run_start",
        "data": json.dumps(run_data.dict())
    })
    
    return {"status": "ok", "run_id": run_data.run_id}

@app.post("/events")
async def ingest_event(event: EventIngest):
    # Push to Redis Stream for async processing
    r.xadd(settings.INGESTION_STREAM_NAME, {
        "type": "event",
        "data": json.dumps(event.dict(), default=str)
    })
    return {"status": "queued", "event_id": event.event_id}

@app.get("/health")
def health():
    return {"status": "healthy"}
