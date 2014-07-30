import MapReduce
import sys

"""
Assume you have two matrices A and B in a sparse matrix format, where each record is of the form i, j, value. Design a MapReduce algorithm to compute the matrix multiplication A x B
"""

mr = MapReduce.MapReduce()
mr2 = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # record: [matrix, i, j, value]
    for i in range(5):
    	if record[0]=="a":
    		mr.emit_intermediate((record[1], i), record)
    	else:
    		mr.emit_intermediate((i, record[2]), record)

def reducer(key, list_of_values):
    # split the list into two parts
    a_value = {}
    b_value = {}
    for record in list_of_values:
    	if record[0]=="a":
    		a_value[record[2]] = record[3]
    	else:
    		b_value[record[1]] = record[3]
    sum = 0
    for col_num in a_value:
    	if col_num in b_value:
    		sum += a_value[col_num]*b_value[col_num]
    mr.emit(key+(sum,))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
