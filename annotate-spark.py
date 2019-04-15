"""
1. Practise and understand the normal annotate.py file and then try to implement
it using Spark

2. The Performance file has multiple entries of loan id form the acquisition file
of a loan which may or may not have been foreclosed upon. Count the performance ie
the occurence of a loan id in the performance file and if it was foreclosed, assign
a new column called foreclosure status and then send this back to the acquisition file
or do a join between the two files

2. From the Performance CSV, do a GroupBy on the id, count of id and the 
foreclosure date indicating the loan was foreclosed and add a status column

"""

from pyspark.sql import SparkSession, SQLContext


spark = SparkSession.builder.appName("Annotate").getOrCreate()
