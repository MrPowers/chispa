
def check_type_equal_ignore_nullable(sf1, sf2):
    """Checks StructField data types ignoring nullables.

    Handles array element types also.
    """
    dt1, dt2 = sf1.dataType, sf2.dataType
    if dt1.typeName() == dt2.typeName():
        # Account for array types by inspecting elementType.
        if dt1.typeName() == 'array':
            return dt1.elementType == dt2.elementType
        else:
            return True
    else:
        return False


def are_structfields_equal(sf1, sf2, ignore_nullability=False):
    if ignore_nullability:
        if sf1 is None and sf2 is not None:
            return False
        elif sf1 is not None and sf2 is None:
            return False
        elif sf1.name != sf2.name or not check_type_equal_ignore_nullable(sf1, sf2):
            return False
        else:
            return True
    else:
        return sf1 == sf2
