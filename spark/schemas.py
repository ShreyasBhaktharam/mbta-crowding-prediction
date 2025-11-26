from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from .utils import SCHEMA_DIR, load_json

_JSON_TYPE_MAP = {
    "string": StringType(),
    "number": DoubleType(),
    "integer": LongType(),
    "boolean": BooleanType(),
}


def _resolve_type(prop: Dict[str, Any]):
    t = prop.get("type")
    if isinstance(t, list):
        # prefer the first non-null entry
        t = next((item for item in t if item != "null"), t[0])
    spark_type = _JSON_TYPE_MAP.get(t)
    if spark_type:
        return spark_type
    if t == "object":
        nested = prop.get("properties", {})
        nested_required = prop.get("required", [])
        fields = [
            StructField(name, _resolve_type(defn), nullable=name not in nested_required)
            for name, defn in nested.items()
        ]
        return StructType(fields)
    if t == "integer":
        return IntegerType()
    raise ValueError(f"Unsupported JSON schema type: {t}")


def _struct_from_json(schema_dict: Dict[str, Any]) -> StructType:
    props = schema_dict.get("properties", {})
    required = schema_dict.get("required", [])
    fields = [
        StructField(name, _resolve_type(prop), nullable=name not in required)
        for name, prop in props.items()
    ]
    return StructType(fields)


@lru_cache(maxsize=None)
def load_schema(topic: str) -> StructType:
    """Load a StructType for a Kafka topic payload."""
    path = SCHEMA_DIR / f"{topic}.value.schema.json"
    if not path.exists():
        raise FileNotFoundError(f"Schema not found for topic {topic}: {path}")
    schema_dict = load_json(path)
    return _struct_from_json(schema_dict)
