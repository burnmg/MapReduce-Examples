import MapReduce
import sys

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):

    if record[0] == 'a':
        for j in range(0, 5):
            mr.emit_intermediate((record[1], j), ('a', record[2], record[3]))
    else:
        for i in range(0, 5):
            mr.emit_intermediate((i, record[2]), ('b', record[1], record[3]))


def reducer(key, list_of_values):

    a_vals = [0]*5
    b_vals = [0]*5
    for item in list_of_values:

        m, p, val = item

        if m == 'a':
            a_vals[p] = val
        else:
            b_vals[p] = val

    sum = 0
    for (index, val) in enumerate(a_vals):
        sum += val * b_vals[index]

    i, j = key
    mr.emit((i, j, sum))



# Do not modify below this line('b', i, record[3])
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
