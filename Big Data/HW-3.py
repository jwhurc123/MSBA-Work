#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function



# import necessary packages

from pyspark.sql import SparkSession
import sys




# create spark session and read text file into rdd

spark = SparkSession.builder.appName("HW3").getOrCreate()

#   sys.argv[0] is the name of the script.
#   sys.argv[1] is the input_path
#   sys.argv[2] is the output_path

# reads input into an rdd

input_path = sys.argv[1] # input file

print("input_path: ", input_path)

rdd = spark.sparkContext.textFile(input_path)




# function to convert input text file to (word,1) key/value pair

def word_transformer(row):
    
    # split the text file into words
    tokens = row.split(" ")
    # create set of of special characters that are allowed
    char = {".",";",",","?",":"}
    key_value = []
    
    # for loop to iterate through each word
    for word in tokens:
        # ignores all words less than 5 characters long
        if len(word) >= 5:
            # condition for words with special characters at the end
            if word[-1] in char:
                # condition to ignore words with special characters throughout word
                if word[:-1].isalpha() == True:
                    key_value.append((word[:-1].lower(),1))
            # for words that don't have special characters at the end
            else:
                # condition to ignore words with special characters throughout word
                if word.isalpha() == True:
                    key_value.append((word.lower(),1))
    return key_value




# apply word_transformer function throughout text

rdd_transformed = rdd.flatMap(word_transformer)




# reduceByKey to obtain sum of count per word key

rdd_count = rdd_transformed.reduceByKey(lambda x, y: x + y)




# create and print top_10 list of most occurring words

top_10 = rdd_count.sortBy(lambda x: x[1], ascending=False).take(10)

for (word, count) in top_10:
	print("(%s,%i)" % (word, count))

spark.stop()
