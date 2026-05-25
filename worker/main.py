import redis
import json
import time
from typing import Dict, Any
from neo4j import GraphDatabase
from ..shared.config import settings
from ..shared.database import SessionLocal
from ..shared import models

from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models

class TrajectoryWorker:
    def __init__(self):
        self.redis = redis.from_url(settings.REDIS_URL)
        self.neo4j_driver = GraphDatabase.driver(
            settings.NEO4J_URL, 
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
        )
        self.qdrant = QdrantClient(url=settings.QDRANT_URL)
        self.stream_name = settings.INGESTION_STREAM_NAME
        self.group_name = "processing_group"
        self.consumer_name = "worker_1"
        
        # Setup Qdrant Collection
        try:
            self.qdrant.create_collection(
                collection_name="trajectory_states",
                vectors_config=qdrant_models.VectorParams(size=384, distance=qdrant_models.Distance.COSINE),
            )
        except Exception:
            pass # Already exists

        # Setup Redis Consumer Group
        try:
            self.redis.xgroup_create(self.stream_name, self.group_name, id="0", mkstream=True)
        except redis.exceptions.ResponseError:
            # Group already exists
            pass

    def process_event(self, event_data: Dict[str, Any]):
        db = SessionLocal()
        try:
            # 1. Write to Postgres
            event = models.Event(
                id=event_data["event_id"],
                run_id=event_data["run_id"],
                timestamp=event_data["timestamp"],
                event_type=event_data["event_type"],
                name=event_data["name"],
                data=event_data["data"],
                parent_id=event_data.get("parent_id")
            )
            db.add(event)
            db.commit()
            
            # 2. Write to Neo4j
            with self.neo4j_driver.session() as session:
                # Create Node
                session.run(
                    """
                    MERGE (e:Event {id: $id})
                    SET e.name = $name,
                        e.type = $type,
                        e.timestamp = $timestamp,
                        e.run_id = $run_id
                    """,
                    id=event_data["event_id"],
                    name=event_data["name"],
                    type=event_data["event_type"],
                    timestamp=event_data["timestamp"],
                    run_id=event_data["run_id"]
                )
                
                # Create Causal Edge if parent exists
                if event_data.get("parent_id"):
                    session.run(
                        """
                        MATCH (parent:Event {id: $parent_id})
                        MATCH (child:Event {id: $child_id})
                        MERGE (parent)-[:CAUSES]->(child)
                        """,
                        parent_id=event_data["parent_id"],
                        child_id=event_data["event_id"]
                    )
            
            # 3. Store in Qdrant (Placeholder for real embedding)
            # In a real scenario, we would use a model to embed the 'data' or 'name'
            # Here we use a deterministic dummy vector based on the event name
            import hashlib
            name_hash = hashlib.md5(event_data["name"].encode()).digest()
            # Convert hash to a 384-dimensional vector (repeating and slicing)
            dummy_vector = [(b / 255.0) for b in (name_hash * 24)[:384]]
            
            self.qdrant.upsert(
                collection_name="trajectory_states",
                points=[
                    qdrant_models.PointStruct(
                        id=event_data["event_id"],
                        vector=dummy_vector,
                        payload=event_data
                    )
                ]
            )

            # 4. Hindsight Credit Assignment (Simplified)
            # If this is a FINISH event, we could trigger a full trajectory analysis
            if event_data["event_type"] == "finish":
                self.compute_credit_assignment(event_data["run_id"])

            print(f"[Worker] Processed event: {event_data['event_id']}")
        except Exception as e:
            print(f"[Worker] Error processing event: {e}")
            db.rollback()
        finally:
            db.close()

    def compute_credit_assignment(self, run_id: str):
        print(f"[Worker] Computing credit assignment for run: {run_id}")
        # Simplified logic: Tag nodes in Neo4j with importance scores
        with self.neo4j_driver.session() as session:
            session.run(
                """
                MATCH (e:Event {run_id: $run_id})
                SET e.credit_score = rand() // Placeholder for actual causal importance
                """,
                run_id=run_id
            )

    def run(self):
        print(f"[Worker] Starting consumer for {self.stream_name}...")
        while True:
            try:
                # Read from stream
                messages = self.redis.xreadgroup(
                    self.group_name, self.consumer_name, {self.stream_name: ">"}, count=10, block=5000
                )
                
                for stream, msg_list in messages:
                    for msg_id, payload in msg_list:
                        msg_type = payload[b"type"].decode()
                        data = json.loads(payload[b"data"].decode())
                        
                        if msg_type == "event":
                            self.process_event(data)
                        
                        # Acknowledge message
                        self.redis.xack(self.stream_name, self.group_name, msg_id)
            except Exception as e:
                print(f"[Worker] Loop error: {e}")
                time.sleep(1)

if __name__ == "__main__":
    worker = TrajectoryWorker()
    worker.run()
