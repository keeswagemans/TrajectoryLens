from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Any, Optional
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from ..shared.database import get_db
from ..shared import models
from ..shared.config import settings

app = FastAPI(title="TrajectoryLens API Service")

neo4j_driver = GraphDatabase.driver(
    settings.NEO4J_URL, 
    auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
)

qdrant_client = QdrantClient(url=settings.QDRANT_URL)

@app.get("/projects")
def list_projects(db: Session = Depends(get_db)):
    return db.query(models.Project).all()

@app.get("/projects/{project_id}/runs")
def list_runs(project_id: str, db: Session = Depends(get_db)):
    return db.query(models.Run).filter(models.Run.project_id == project_id).all()

@app.get("/runs/{run_id}")
def get_run(run_id: str, db: Session = Depends(get_db)):
    run = db.query(models.Run).filter(models.Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run

@app.get("/runs/{run_id}/graph")
def get_run_graph(run_id: str):
    with neo4j_driver.session() as session:
        result = session.run(
            """
            MATCH (e:Event {run_id: $run_id})
            OPTIONAL MATCH (e)-[r:CAUSES]->(child)
            RETURN e, r, child
            """,
            run_id=run_id
        )
        
        nodes = []
        edges = []
        seen_nodes = set()
        
        for record in result:
            node = record["e"]
            if node.id not in seen_nodes:
                nodes.append({
                    "id": node["id"],
                    "label": node["name"],
                    "type": node["type"],
                    "timestamp": node["timestamp"]
                })
                seen_nodes.add(node.id)
            
            if record["child"]:
                child = record["child"]
                edges.append({
                    "source": node["id"],
                    "target": child["id"],
                    "type": "CAUSES"
                })
        
        return {"nodes": nodes, "edges": edges}

@app.get("/events/{event_id}/counterfactuals")
async def get_counterfactuals(event_id: str, limit: int = 5):
    """
    Find 'alternate timelines' by searching for similar events in other runs.
    """
    try:
        # 1. Get the vector for the current event
        points = qdrant_client.retrieve(
            collection_name="trajectory_states",
            ids=[event_id],
            with_vectors=True
        )
        
        if not points:
            raise HTTPException(status_code=404, detail="Event vector not found")
        
        target_vector = points[0].vector
        
        # 2. Search for similar events (excluding the current run to find true alternates)
        current_run_id = points[0].payload["run_id"]
        
        similar_events = qdrant_client.search(
            collection_name="trajectory_states",
            query_vector=target_vector,
            limit=limit + 1, # +1 in case the original event is returned
            with_payload=True
        )
        
        # 3. Filter out events from the same run
        alternates = [
            e.payload for e in similar_events 
            if e.payload["run_id"] != current_run_id
        ]
        
        # 4. For each alternate, we could potentially fetch its "future" from Neo4j
        # For now, we just return the similar events
        return {
            "original_event_id": event_id,
            "alternates": alternates[:limit]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "healthy"}
