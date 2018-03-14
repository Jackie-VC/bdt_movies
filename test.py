
import sys
from pyspark import SparkContext

if len(sys.argv) != 2:
        print >> sys.stderr, "Usage:  <file>"
        exit(-1)

print(sys.argv[1])
sc = SparkContext(appName="movie stream")
lines = sc.textFile(sys.argv[1], 1)
counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda a, b: a + b)
output = counts.collect()
for (word, count) in output:
    print "%s: %i" % (word, count)
 
sc.stop()
