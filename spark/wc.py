#!/usr/bin/env python
from pyspark import SparkContext

sc = SparkContext()

text_file = sc.textFile("/data/posts.xml")




counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("/posts.txt")


