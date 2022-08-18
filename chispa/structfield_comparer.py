from chispa.schema_comparer import are_structfields_equal_ignore_nullable

def are_structfields_equal(sf1, sf2, ignore_nullability=False):
    if ignore_nullability:
        return are_structfields_equal_ignore_nullable(sf1, sf2)
    else:
        return sf1 == sf2
