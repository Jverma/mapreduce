import MapReduce
import sys

"""
Assymetric Friendship Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # record : [user, friend]
    # key: user
    # value: friend
    key = tuple(sorted(record))
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: (user, friend)
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
        total +=v
    if total == 1:
        rev_key = key[::-1]
        mr.emit((key))
        mr.emit((rev_key))
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
