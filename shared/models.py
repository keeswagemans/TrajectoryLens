from sqlalchemy import Column, String, DateTime, JSON, ForeignKey, Text
from sqlalchemy.orm import relationship
from .database import Base
import uuid
from datetime import datetime

class Project(Base):
    __tablename__ = "projects"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    runs = relationship("Run", back_populates="project")

class Run(Base):
    __tablename__ = "runs"
    id = Column(String, primary_key=True) # Usually provided by SDK
    project_id = Column(String, ForeignKey("projects.id"))
    agent_name = Column(String)
    task = Column(Text)
    status = Column(String, default="running") # running, completed, failed
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    metadata_json = Column(JSON, default=dict)
    
    project = relationship("Project", back_populates="runs")
    events = relationship("Event", back_populates="run")

class Event(Base):
    __tablename__ = "events"
    id = Column(String, primary_key=True) # event_id from SDK
    run_id = Column(String, ForeignKey("runs.id"), index=True)
    timestamp = Column(DateTime)
    event_type = Column(String)
    name = Column(String)
    data = Column(JSON)
    parent_id = Column(String, index=True, nullable=True)
    
    run = relationship("Run", back_populates="events")
