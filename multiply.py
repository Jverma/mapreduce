import MapReduce
import sys

"""
Matrix Multiplication in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
dict_a = {}
dict_b = {}

def mapper(record):
    # record : [Matrix_id, row_no, col_no, entry] 
    key = record[0]
    key1 = (record[1], record[2])
    value = int(record[3])
    new_value = str(key) + str(record[1]) + str(record[2])
    #value1 = record[1:len(record)]
    if key == 'a':
        for k in range(0,5):
            new_key = (record[1], k)
            mr.emit_intermediate(new_key,{new_value:value})
    else:
        for k in range(0,5):
            new_key = (k, record[2])
            mr.emit_intermediate(new_key, {new_value :value})
            

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    i = key[0]
    k = key[1]
    new_dict = {}
    for v in list_of_values:
        new_dict.update(v)
    #print new_dict
    sum = 0    
    for j in range(0,5):
        a = 'a' + str(i) + str(j)
        b = 'b' + str(j) + str(k)
        if not(a in new_dict):
            a_value = 0
        else:
            a_value = new_dict[a]
        if not(b in new_dict):
            b_value = 0
        else:
             b_value = new_dict[b]
        sum = sum + a_value * b_value      
    
    mr.emit((key[0], key[1], sum))
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
