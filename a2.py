#####################
#   KRISHNA PRASAD  #
#   PORANDLA        #
#   KPORANDL        #
#####################

from pyspark import SparkConf, SparkContext
import time
import sys

#Cleaning string inputs to format the data
def cleanLines(line):
    l = line.split(" ")
    vertices = [int(x) for x in l]
    u = vertices[0]
    v = vertices[1]
    return [(u,v)]

#For every vertex, emit the child and parent of that vertex with markings(to distinguish who is parent and who is child)
def mapParentAndChild(input):
    u = input[0]
    v = input[1]
    return [(u,str(v)+' o'),(v,str(u)+' i')]

#Logic to point child vertices of the current vertex to the parent of that vertex
def emitNextEdges(data):
    b = data[1]
    in_vertices = list(filter(lambda i: i.split(' ')[1] == 'i', b))
    out_vertices = list(filter(lambda i: i.split(' ')[1] == 'o', b))
    result_edges = []
    if len(in_vertices)>0 and len(out_vertices)>0:
        parent = int(out_vertices[0].split(' ')[0])
        for i in range(len(in_vertices)):
            result_edges.append((int(in_vertices[i].split(' ')[0]), parent))
    return result_edges


#Returns parent of the current vertex
def getParent(a):
    return [a[1]]


#Method used in combineByKey to combine values as list
def to_list(a):
    return [a]

#Method used in combineByKey to combine values as list
def append(a,b):
    a.append(b)
    return a

#Method used in combineByKey to combine values as list
def extend(a,b):
    a.extend(b)
    return a


if __name__ == "__main__":
    conf = SparkConf().setAppName("IntroPDP_A2_kporandl")
    a = time.time()
    sc = SparkContext(conf=conf)

    #We define this in the Spark ags. Usually, spark.default.parallelism=numOfExecutoes x numOfCoresPerExecutor
    numPartitions = int(sc.getConf().get("spark.default.parallelism"))

    firstIteration = True

    #Variables to manage break condition
    oldSum = -1
    newSum = 0

    while(oldSum!=newSum):
        if firstIteration:
            output = sc.textFile(sys.argv[1]).repartition(numPartitions).flatMap(cleanLines).flatMap(mapParentAndChild).combineByKey(to_list, append, extend).flatMap(emitNextEdges)
            newSum = output.flatMap(getParent).sum()
            firstIteration = False
        else:
            oldSum = newSum
            output = output.flatMap(mapParentAndChild).combineByKey(to_list, append, extend).flatMap(emitNextEdges)
            newSum = output.flatMap(getParent).sum()

    output.saveAsTextFile(sys.argv[2])
    sc.stop()
    b = time.time()
    print(format(b-a, ".1f"))

