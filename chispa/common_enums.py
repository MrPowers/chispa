from __future__ import annotations

from enum import Enum


class OutputFormat(str, Enum):
    TABLE = "table"
    TREE = "tree"


class TypeName(str, Enum):
    ARRAY = "array"
    STRUCT = "struct"
