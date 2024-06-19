#!/usr/bin/env python
# coding: utf-8


### Import PySpark and read input file as a DataFrame
from pyspark.sql import SparkSession
import sys



# create sparkSession instance
spark = SparkSession.builder.appName("HW4").getOrCreate()

# remove INFO logging from executing spark-session job on terminal
spark.sparkContext.setLogLevel("ERROR")

input_path = sys.argv[1] # input csv file



# create DataFrame from input path
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(input_path)



# convert all column headers to lowercase
for col in df.columns:
    df = df.withColumnRenamed(col,col.lower())


# create temporary view df1 to execute SQL query
df.createOrReplaceTempView("df1")


# SQL query to create df2, where all missing namelast ans visitee_namelast values are removed
df2 = spark.sql("SELECT * FROM df1 WHERE (namelast IS NOT NULL AND visitee_namelast IS NOT NULL)")




### create visitor and visitee columns in df2 DataFrame following the (lastname, firstname) format


# import necessary packages
from pyspark.sql.functions import lower, concat, lit


# add the concatenated columns to df2
df2 = df2.withColumn('visitor', concat(lower(df.namelast),lit(', '),lower(df.namefirst)))

df2 = df2.withColumn('visitee',concat(lower(df.visitee_namelast),lit(', '),lower(df.visitee_namefirst)))



### group visitors (names are lower case) by frequency, and then show the top 10 most frequent visitors

# create dataframe to group visitors by count, and then order the results descending
visitor_count = df2.groupBy('visitor').count().orderBy("count",ascending=False)

# show top 10 rows of visitor dataframe 
print("Top 10 Most Frequent Visitors")
visitor_count.show(10)



### group visitees (names are lower case) by frequency, and then show the top 10 most frequently visited people

# create dataframe to group visitees by count, and then order the results descending
visitee_count = df2.groupBy('visitee').count().orderBy("count",ascending=False)

# show top 10 rows of visitee dataframe 
print("Top 10 Most Frequent Visitees")
visitee_count.show(10)



### group visitor/visitee combos by frequency, and then show the top 10 most frequent visitor/visitee combo

# create dataframe to group visitees by count, and then order the results descending
visitor_visitee_count = df2.groupBy(['visitor','visitee']).count().orderBy("count",ascending=False)


# show top 10 rows of visitor/visitee dataframe 
print("Top 10 Most Frequent Visitor/Visitee combos")
visitor_visitee_count.show(10)



# SQL query to show count of records dropped from table df1
print("Total Records Dropped")
spark.sql("SELECT COUNT(*) AS Dropped_Records_Count FROM df1 WHERE (namelast IS NULL OR visitee_namelast IS NULL)").show()


