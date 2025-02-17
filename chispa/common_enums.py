from enum import Enum


class OutputFormat(Enum):
    TABLE = "table"
    TREE = "tree"


class TypeName(Enum):
    ARRAY = "array"
    STRUCT = "struct"
