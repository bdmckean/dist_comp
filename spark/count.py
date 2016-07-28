#!/usr/bin/env python
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row


if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    text_file = sc.textFile("/data/post_body.txt")

    
    d = [] 
    # Creates a DataFrame having a single column named "line"
    df = text_file.map(lambda r: Row(r)).toDF(["line"])
    df.cache()
    cpp_lines = df.filter(df["line"].like("%c++%"))
    d.append(('C++',cpp_lines.count()))
    python_lines = df.filter(df["line"].like("%python%"))
    d.append(('python',python_lines.count()))
    perl_lines = df.filter(df["line"].like("%perl%"))
    d.append(('perl',perl_lines.count()))
    skala_lines = df.filter(df["line"].like("%skala%"))
    d.append(('skala',skala_lines.count()))
    c_lines = df.filter(df["line"].like("% c %"))
    d.append(('c',c_lines.count()))
    java_lines = df.filter(df["line"].like("%java%"))
    d.append(('java',java_lines.count()))

    for item in d:
        print(item[0], item[1])
