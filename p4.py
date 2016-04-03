import MapReduce
import sys

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):

    person = record[0]
    friend = record[1]

    mr.emit_intermediate(person, record)
    mr.emit_intermediate(friend, record)


def reducer(person, list_of_values):

    person_friend_pairs = []
    friend_person_pairs = []

    for p in list_of_values:
        if person == p[0]:
            person_friend_pairs.append(p)
        else:
            friend_person_pairs.append(p)

    for p in person_friend_pairs:
        if [p[1], p[0]] not in friend_person_pairs:
            mr.emit((p[1], p[0]))
            mr.emit((p[0], p[1]))



# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
