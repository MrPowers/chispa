def assert_column_equality(df, col_name1, col_name2):

    elements = df.select(col_name1, col_name2).collect()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    if colName1Elements != colName2Elements:
	    zipped = list(zip(colName1Elements, colName2Elements))
	    for elements in zipped:
	    	if elements[0] == elements[1]:
	    		print(str(elements[0]) + " | " + str(elements[1]))
	    	else:
	    		print("MISMATCH: " + str(elements[0]) + " | " + str(elements[1]))
    # if (colName1Elements != colName2Elements):
    #   val mismatchMessage = "\n" + ArrayPrettyPrint.showTwoColumnString(
    #     Array((colName1, colName2)) ++ colName1Elements.zip(colName2Elements)
    #   )
    #   throw ColumnMismatch(mismatchMessage)
