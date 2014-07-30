import MapReduce
import sys

"""
Write a MapReduce query to remove the last 10 characters from each string of nucleotides, then remove any duplicates generated.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # record: [sequence id, nucleotides]
    key = record[1]
    mr.emit_intermediate(key[:-10], 1)

def reducer(key, list_of_values):
    # key: sequence
    # value: list of occurrence counts, won't use
    mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
