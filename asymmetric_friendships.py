import MapReduce
import sys

"""
The relationship "friend" is often symmetric, meaning that if I am your friend, you are my friend. Implement a MapReduce algorithm to check whether this property holds. Generate a list of all non-symmetric friend relationships.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # record: [personA, personB]
    mr.emit_intermediate(record[0], record)
    mr.emit_intermediate(record[1], record)

def reducer(key, list_of_values):
    # key: person
    # value: list of friend relationships
    # should have lots of duplicates
    friends = set()
    hasPersonAsFriend = set()
    for f in list_of_values:
      if f[0]==key:
        friends.add(f[1])
      else:
        hasPersonAsFriend.add(f[0])
    for f in friends:
      if f not in hasPersonAsFriend:
        mr.emit((key, f))
        mr.emit((f, key))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
