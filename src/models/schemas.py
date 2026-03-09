from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class StorageType(str, Enum):
    table = "table"
    file = "file"
    stream = "stream"
    api = "api"


class ModuleNode(BaseModel):
    path: str
    language: str
    purpose_statement: str = ""
    domain_cluster: str = "unknown"
    complexity_score: float = 0.0
    change_velocity_30d: int = 0
    is_high_velocity_core: bool = False
    is_dead_code_candidate: bool = False
    last_modified: str = ""
    imports: list[str] = Field(default_factory=list)
    public_functions: list[str] = Field(default_factory=list)
    classes: list[str] = Field(default_factory=list)
    class_inheritance: dict[str, list[str]] = Field(default_factory=dict)
    loc: int = 0
    comment_ratio: float = 0.0


class DatasetNode(BaseModel):
    name: str
    storage_type: StorageType = StorageType.table
    schema_snapshot: dict[str, Any] = Field(default_factory=dict)
    freshness_sla: str = ""
    owner: str = ""
    is_source_of_truth: bool = False


class FunctionNode(BaseModel):
    qualified_name: str
    parent_module: str
    signature: str = ""
    purpose_statement: str = ""
    call_count_within_repo: int = 0
    is_public_api: bool = False


class TransformationNode(BaseModel):
    source_datasets: list[str]
    target_datasets: list[str]
    transformation_type: str
    source_file: str
    line_range: tuple[int, int] = (0, 0)
    sql_query_if_applicable: str = ""


class EdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    PRODUCES = "PRODUCES"
    CONSUMES = "CONSUMES"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"


class GraphEdge(BaseModel):
    source: str
    target: str
    edge_type: EdgeType
    weight: float = 1.0
    metadata: dict[str, Any] = Field(default_factory=dict)


class KnowledgeGraphSnapshot(BaseModel):
    node_count: int
    edge_count: int
    node_types: dict[str, int] = Field(default_factory=dict)
    edge_types: dict[str, int] = Field(default_factory=dict)


class ContextWindowBudget(BaseModel):
    model_fast: str = "gemini-flash"
    model_synth: str = "gpt-4o"
    estimated_tokens: int = 0
    spent_tokens: int = 0
    estimated_cost_usd: float = 0.0
    spent_cost_usd: float = 0.0


class DayOneAnswer(BaseModel):
    question_id: str
    answer: str
    evidence: list[dict[str, Any]] = Field(default_factory=list)


class TraceEvent(BaseModel):
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    agent: str
    action: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    confidence: Literal["low", "medium", "high"] = "medium"
