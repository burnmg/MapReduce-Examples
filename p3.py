import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):

    person = record[0]
    friend = record[1]

    mr.emit_intermediate(person, 1)

def reducer(key, list_of_values):


    friends_count = sum(list_of_values)

    mr.emit((key, friends_count))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
