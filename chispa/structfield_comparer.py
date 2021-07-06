import six
from pyspark.sql.types import StructType

def are_structfields_equal(sf1, sf2, ignore_nullability=False):
    if ignore_nullability:
        if sf1 is None and sf2 is not None:
            return False
        elif sf1 is not None and sf2 is None:
            return False
        elif sf1.name != sf2.name:
            return False
        elif isinstance(sf1.dataType, StructType) and isinstance(sf2.dataType, StructType):
            for t1, t2 in six.moves.zip_longest(sf1.dataType, sf2.dataType):
                if not are_structfields_equal(t1, t2, ignore_nullability):
                    return False
            return True
        elif sf1.dataType != sf2.dataType:
            return False
        else:
            return True
    else:
        return sf1 == sf2
