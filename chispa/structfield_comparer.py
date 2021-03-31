def are_structfields_equal(sf1, sf2, ignore_nullability=False):
    if ignore_nullability:
        if sf1 is None and sf2 is not None:
            return False
        elif sf1 is not None and sf2 is None:
            return False
        elif sf1.name != sf2.name or sf1.dataType != sf2.dataType:
            return False
        else:
            return True
    else:
        return sf1 == sf2
