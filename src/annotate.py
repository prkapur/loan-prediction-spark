"""
The Performance file has multiple entries of loan id form the acquisition file
of a loan which may or may not have been foreclosed upon. Count the performance ie
the occurence of a loan id in the performance file and if it was foreclosed, assign
a new column called foreclosure status and then send this back to the acquisition file
or do a join between the two files

# from performance data, convert foreclosure date into status
- This can be done using a udf
# fill na with -1 
# convert categorical column to a numeric column Wait

reead multiple files and load into one dataframe? 
 df = spark.read.format('json').load(['python/test_support/sql/people.json',
    'python/test_support/sql/people1.json'])
>df.dtypes
[('age', 'bigint'), ('aka', 'string'), ('name', 'string')]
df = spark.read.format('csv').load(["data/Acquisition_2012Q1.txt","data/Acquisition_2012Q2.txt"])
single_df = spark.read.format('csv').load("data/Acquisition_*.txt")
"""

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf,split
from pyspark.sql.types import *

def date_to_status(value):
# """Defining a udf to convert foreclosure_date into
# foreclosure_status, if the value of string date is
# null, assign it 0, it a date is present assign it 1"""

    if  value == "": 
        return 0
    else: 
       return 1

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Annotate").getOrCreate()

    acquisition = spark.read.option("sep",",").csv("processed/acquisition.csv",header="True")
    performance = spark.read.option("sep",",").csv("processed/performance.csv",header="True")

    # Move this to a function
    acquisition = acquisition.withColumn("first_payment_month",split("first_payment_date","/").getItem(0))
    acquisition = acquisition.withColumn("first_payment_year",split("first_payment_date","/").getItem(1))
    acquisition = acquisition.withColumn("origination_month",split("origination_date","/").getItem(0))
    acquisition = acquisition.withColumn("origination_year",split("origination_date","/").getItem(0))

    #assign a new column called foreclosure_status 
    udfDateToStatus = udf(date_to_status, StringType())
    performance = performance.withColumn("foreclosure_status", udfDateToStatus("foreclosure_date"))

    df1 = acquisition.alias("df1")
    df2 = performance.alias("df2")
    train = df1.join(df2,df1.id == df2.id, how = "left").select("df1.*","df2.foreclosure_status")
    train = train.na.fill("0")

    # train.coalesce(1).write.option("header","true").format("csv").save("train.csv")

    df = train.select("*")

    # df = df.withColumn("balance_double", df["balance"].cast(DoubleType()))
    # df = df.withColumn("loan_term_int", df["loan_term"].cast(IntegerType()))
    # df = df.withColumn("ltv_int", df["ltv"].cast(IntegerType()))
    # df = df.withColumn("cltv_int", df["cltv"].cast(IntegerType()))
    # df = df.withColumn("borrower_count_int", df["borrower_count"].cast(IntegerType()))
    # df = df.withColumn("dti_int", df["dti"].cast(IntegerType()))
    # df = df.withColumn("borrower_credit_score_int", df["borrower_credit_score"].cast(IntegerType()))
    # df = df.withColumn("unit_count_int", df["unit_count"].cast(IntegerType()))
    # df = df.withColumn("zip_int", df["zip"].cast(IntegerType()))
    # df = df.withColumn("insurance_percentage_double", df["insurance_percentage"].cast(IntegerType()))
    # df = df.withColumn("foreclosure_status_int", df["foreclosure_status"].cast(IntegerType()))

    df.coalesce(1).write.option("header","true").format("parquet").save("train.parquet")