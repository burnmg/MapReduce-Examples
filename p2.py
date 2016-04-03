import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):

    key = record[1]

    mr.emit_intermediate(key, record)


def reducer(key, list_of_values):

    line_items = []
    orders = []

    for v in list_of_values:
        if v[0] == "line_item":
            line_items.append(v)
        else:
            orders.append(v)

    joined_records = []

    for line_item in line_items:
        for order in orders:
            joined_record = order + line_item
            joined_records.append(joined_record)

    for jr in joined_records:

        mr.emit(jr)


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
