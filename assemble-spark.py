"""
1. Combine all the Performance files into one file
2. Combine all the Acquisition files into one file
3. Write the 2 output files as Performance.csv and Acquisition.csv

filter, select, groupby
"""

from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, LongType
from pyspark.sql.functions import *

#When Loading id as IntegerType, the schema returns null
#Load id as LongType and the schema load works

spark = SparkSession.builder.appName("Assemble").getOrCreate()

data_schema_acquisition = [
StructField("id",LongType(),True),
StructField("channel",StringType(),False),
StructField("seller",StringType(),False),
StructField("interest_rate",FloatType(),False),
StructField("balance",LongType(),False),
StructField("loan_term",IntegerType(),False),
StructField("origination_date",StringType(),False),
StructField("first_payment_date",StringType(),False),
StructField("ltv",IntegerType(),False),
StructField("cltv",IntegerType(),False),
StructField("borrower_count",IntegerType(),False),
StructField("dti",IntegerType(),False),
StructField("borrower_credit_score",IntegerType(),False),
StructField("first_time_homebuyer",StringType(),False),
StructField("loan_purpose",StringType(),False),
StructField("property_type",StringType(),False),
StructField("unit_count", IntegerType(),False),
StructField("occupancy_status",StringType(),False),
StructField("property_state",StringType(),False),
StructField("zip",StringType(),False),
StructField("insurance_percentage",StringType(),False),
StructField("product_type",StringType(),False),
StructField("co_borrower_credit_score",StringType(),False)
]


data_schema_performance = [
    StructField("id",LongType(),False),
    StructField("reporting_period",StringType(),True),
    StructField("servicer_name",StringType(),True),
    StructField("interest_rate",StringType(),False),
    StructField("balance",LongType(),False),
    StructField("loan_age",IntegerType(),False),
    StructField("months_to_maturity",IntegerType(),False),
    StructField("msa",StringType(),False),
    StructField("delinquency_status",StringType(),False),
    StructField("modification_flag",StringType(),False),
    StructField("zero_balance_code",StringType(),False),
    StructField("zero_balance_date",StringType(),False),
    StructField("last_paid_installment_date",StringType(),False),
    StructField("foreclosure_date",StringType(),False),
    StructField("disposition_date",StringType(),False),
    StructField("foreclosure_costs",StringType(),False),
    StructField("property_repair_costs",StringType(),False),
    StructField("recovery_costs",StringType(),False),
    StructField("misc_costs",StringType(),False),
    StructField("tax_costs",StringType(),False),
    StructField("sale_proceeds",StringType(),False),
    StructField("credit_enhancement_proceeds",StringType(),False),
    StructField("repurchase_proceeds",StringType(),False),
    StructField("other_foreclosure_proceeds",StringType(),False),
    StructField("non_interest_bearing_balance",StringType(),False),
    StructField("principal_forgiveness_balance",StringType(),False),
]

final_structure_acquisition  = StructType(fields=data_schema_acquisition)
lines_acq = spark.read.option("sep","|").csv("data/Acquisition_2012Q1.txt",header="False", nullValue = "NA", schema=final_structure_acquisition)
#lines_acq.show(10)

final_structure_performance = StructType(fields=data_schema_performance)
lines_per = spark.read.option("sep","|").csv("data/Performance_2012Q1.txt",header="False", nullValue = "NA",schema=final_structure_performance)
#lines_per.show(10)

print("Selecting the necessary columns only from performance table")

# Select All lines where foreclosure is not null

acquisition = lines_acq
performance = lines_per.select("id","foreclosure_date").filter("foreclosure_date IS NOT NULL")

#creating a temporary performance_table
#lines_per.createOrReplaceTempView("performance_table")
#results = spark.sql("select id,foreclosure_date from performance_table where foreclosure_date is not null")#.show()

#acquisition.coalesce(1).write.option("header","true").csv("Acquisition.csv")
#performance.coalesce(1).write.option("header","true").csv("Performance.csv")

#Join the two dataframes i.e performance and acquisition

print("Printing the joined dataframes")

df1 = acquisition.alias('df1')
df2 = performance.alias('df2')
joinedDataSet = df1.join(df2, df1.id == df2.id, how='left').select("df1.*","df2.foreclosure_date")

#joinedDataset.coalesce(1).write.option("header","true").csv("JoinedData.csv")

#joinedDataset.write.partitionBy("id").parquet("train")

#rdd = joinedDataset.rdd.map(list)
#print(rdd.collect())

joinedDataSet = joinedDataSet.withColumn("month",split("foreclosure_date","/").getItem(0))
joinedDataSet = joinedDataSet.withColumn("year",split("foreclosure_date","/").getItem(1))

joinedDataSet.show()

# split_col = spark.sql.functions.split(joinedDataSet['foreclosure_date'], '/')
# joinedDataSet = joinedDataSet.withColumn('NAME1', split_col.getItem(0))
# joinedDataSet = joinedDataSet.withColumn('NAME2', split_col.getItem(1))

