from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StorageType(str, Enum):
    table = "table"
    file = "file"
    stream = "stream"
    api = "api"


class NodeType(str, Enum):
    module = "module"
    dataset = "dataset"
    function = "function"
    transformation = "transformation"


class ModuleNode(BaseModel):
    model_config = ConfigDict(extra="allow")

    path: str
    language: str
    purpose_statement: str = ""
    domain_cluster: str = "unknown"
    complexity_score: float = 0.0
    change_velocity_30d: int = 0
    velocity_rank_30d: int = 0
    is_high_velocity_core: bool = False
    is_dead_code_candidate: bool = False
    dead_code_symbols: list[str] = Field(default_factory=list)
    pagerank_score: float = 0.0
    is_in_import_cycle: bool = False
    import_cycle_id: str = ""
    import_cycle_size: int = 0
    import_cycle_members: list[str] = Field(default_factory=list)
    last_modified: str = ""
    imports: list[str] = Field(default_factory=list)
    public_functions: list[str] = Field(default_factory=list)
    classes: list[str] = Field(default_factory=list)
    class_inheritance: dict[str, list[str]] = Field(default_factory=dict)
    loc: int = 0
    comment_ratio: float = 0.0

    @field_validator("path", "language", mode="before")
    @classmethod
    def _required_text(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Field is required and must not be empty.")
        return text

    @field_validator("purpose_statement", "domain_cluster", "last_modified", mode="before")
    @classmethod
    def _optional_text(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("complexity_score", mode="before")
    @classmethod
    def _non_negative_float(cls, value: Any) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, parsed)

    @field_validator("change_velocity_30d", "velocity_rank_30d", "import_cycle_size", "loc", mode="before")
    @classmethod
    def _non_negative_int(cls, value: Any) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 0
        return max(0, parsed)

    @field_validator("comment_ratio", "pagerank_score", mode="before")
    @classmethod
    def _ratio_0_to_1(cls, value: Any) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return 0.0
        return min(1.0, max(0.0, parsed))


class DatasetNode(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    storage_type: StorageType = StorageType.table
    schema_snapshot: dict[str, Any] = Field(default_factory=dict)
    freshness_sla: str = ""
    owner: str = ""
    is_source_of_truth: bool = False

    @field_validator("name", mode="before")
    @classmethod
    def _dataset_name_required(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Dataset name is required.")
        return text

    @field_validator("freshness_sla", "owner", mode="before")
    @classmethod
    def _dataset_optional_text(cls, value: Any) -> str:
        return str(value or "").strip()


class FunctionNode(BaseModel):
    model_config = ConfigDict(extra="allow")

    qualified_name: str
    parent_module: str = ""
    signature: str = ""
    purpose_statement: str = ""
    call_count_within_repo: int = 0
    is_public_api: bool = False

    @field_validator("qualified_name", mode="before")
    @classmethod
    def _function_name_required(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Function qualified_name is required.")
        return text

    @field_validator("parent_module", "signature", "purpose_statement", mode="before")
    @classmethod
    def _function_optional_text(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("call_count_within_repo", mode="before")
    @classmethod
    def _non_negative_call_count(cls, value: Any) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 0
        return max(0, parsed)

    @model_validator(mode="after")
    def _derive_parent_module(self) -> FunctionNode:
        if not self.parent_module and "::" in self.qualified_name:
            self.parent_module = self.qualified_name.split("::", 1)[0]
        if not self.parent_module:
            raise ValueError("FunctionNode requires a parent_module.")
        return self


class TransformationNode(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_datasets: list[str] = Field(default_factory=list)
    target_datasets: list[str] = Field(default_factory=list)
    transformation_type: str = "unknown"
    source_file: str = ""
    line_range: tuple[int, int] = (0, 0)
    sql_query_if_applicable: str = ""

    @field_validator("transformation_type", "source_file", "sql_query_if_applicable", mode="before")
    @classmethod
    def _transformation_text(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("line_range", mode="before")
    @classmethod
    def _normalize_line_range(cls, value: Any) -> tuple[int, int]:
        if value is None:
            return (0, 0)
        if isinstance(value, list):
            value = tuple(value)
        if not isinstance(value, tuple) or len(value) != 2:
            return (0, 0)
        try:
            start = max(0, int(value[0]))
            end = max(0, int(value[1]))
        except (TypeError, ValueError):
            return (0, 0)
        if end < start:
            end = start
        return (start, end)


class EdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    PRODUCES = "PRODUCES"
    CONSUMES = "CONSUMES"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"


class GraphEdge(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    target: str
    edge_type: EdgeType
    weight: float = Field(default=1.0, ge=0.0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source", "target", mode="before")
    @classmethod
    def _edge_endpoints_required(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Edge source and target are required.")
        return text


class ImportsEdge(GraphEdge):
    edge_type: Literal[EdgeType.IMPORTS] = EdgeType.IMPORTS


class ProducesEdge(GraphEdge):
    edge_type: Literal[EdgeType.PRODUCES] = EdgeType.PRODUCES


class ConsumesEdge(GraphEdge):
    edge_type: Literal[EdgeType.CONSUMES] = EdgeType.CONSUMES


class CallsEdge(GraphEdge):
    edge_type: Literal[EdgeType.CALLS] = EdgeType.CALLS


class ConfiguresEdge(GraphEdge):
    edge_type: Literal[EdgeType.CONFIGURES] = EdgeType.CONFIGURES


TypedEdge = ImportsEdge | ProducesEdge | ConsumesEdge | CallsEdge | ConfiguresEdge


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
    confidence: Literal["low", "medium", "high"] = "medium"
    confidence_label: Literal["low", "medium", "high"] = "medium"
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    confidence_factors: dict[str, float] = Field(default_factory=dict)
    confidence_components: dict[str, float] = Field(default_factory=dict)
    confidence_reason: str = ""


class TraceEvent(BaseModel):
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    agent: str
    action: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    confidence: Literal["low", "medium", "high"] = "medium"
