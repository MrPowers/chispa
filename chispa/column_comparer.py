class ColumnsNotEqualError(Exception):
   """The columns are not equal"""
   pass


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def assert_column_equality(df, col_name1, col_name2):

    elements = df.select(col_name1, col_name2).collect()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    if colName1Elements != colName2Elements:
	    zipped = list(zip(colName1Elements, colName2Elements))
	    lines = [""]
	    for elements in zipped:
	    	if elements[0] == elements[1]:
	    		lines.append(bcolors.OKBLUE + str(elements[0]) + " | " + str(elements[1]) + bcolors.ENDC)
	    	else:
	    		lines.append(str(elements[0]) + " | " + str(elements[1]))
	    raise ColumnsNotEqualError("\n".join(lines))
    # if (colName1Elements != colName2Elements):
    #   val mismatchMessage = "\n" + ArrayPrettyPrint.showTwoColumnString(
    #     Array((colName1, colName2)) ++ colName1Elements.zip(colName2Elements)
    #   )
    #   throw ColumnMismatch(mismatchMessage)
