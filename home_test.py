from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)
df = sqlContext.read.format('com.databricks.spark.csv').load('homeAssignment.csv')

letters = {}
for row in df.rdd.toLocalIterator():
    letter = row[0]
    if letter not in letters.keys():
        letters[letter] = []
    letters[letter].append(int(row[1]))
for letter in letters:
    letters[letter].sort()
    f = open((letter+".txt"), 'w')
    for num in letters[letter]:
        f.write(str(num) + '\n')
    f.close()


