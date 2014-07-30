import MapReduce
import sys

"""
Implement a relational join as a MapReduce query
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    #record: a list of strings representing a tuple in the database
    order_id = record[1]
    mr.emit_intermediate(order_id, record)

def reducer(key, list_of_values):
    # key: order_id
    # value: list of tuples containing the order_id
    # to do: a cross product
    list1 = []
    list2 = []
    # classify into two groups
    for t in list_of_values:
        if(t[0]=="order"):
            list1.append(t)
        else:
            list2.append(t)
    # do a cross product
    for t1 in list1:
        for t2 in list2:
            mr.emit(t1+t2)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
