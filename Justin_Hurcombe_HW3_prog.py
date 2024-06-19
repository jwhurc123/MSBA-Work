#!/usr/bin/env python
# coding: utf-8


# Import PySpark and read input file as an rdd

from pyspark.sql import SparkSession
import sys


# create sparkSession instance

spark = SparkSession.builder.appName("top_10_words").getOrCreate()

input_path = "/<directory>/205-0.txt" # input file


# book_rdd: RDD[String]

book_rdd = spark.sparkContext.textFile(input_path)


# function to convert input text file to (word,1) key/value pair

def word_transformer(row):
    
    # split the text file into words
    tokens = row.split()
    
    # create set of of special characters that are allowed
    char = {".",";",",","?",":"}
    
    # create empty list to be filled with key/value pairs
    key_value = []
    
    # for loop to iterate through each word and create key/value pairs to fill list
    for word in tokens:
        # condition to ignore all words less than 5 characters long
        if len(word) >= 5:
            # condition to accept words with allowed special characters at the end
            if word[-1] in char:
                # condition to ignore words with special characters throughout word
                if word[:-1].isalpha() == True:
                    key_value.append((word[:-1].lower(),1))
            # for words that don't have special characters at the end
            else:
                # condition to ignore words with special characters throughout word
                if word.isalpha() == True:
                    key_value.append((word.lower(),1))
    # returns key/value pair list for row
    return key_value


# apply word_transformer function throughout text; output will be a flat list of (key, 1) pairs

rdd_transformed = book_rdd.flatMap(word_transformer)


# reduceByKey to obtain sum of count per word key; output would be flat list of (key, count) pairs

rdd_count = rdd_transformed.reduceByKey(lambda x, y: x + y)


# create top_10 list of most occurring words

top_10 = rdd_count.sortBy(lambda x: x[1], ascending=False).take(10)


# print top_10 words and their word counts

for (word, count) in top_10:
    print("(%s, %i)" % (word, count))


# stop sparkSession

spark.stop()
