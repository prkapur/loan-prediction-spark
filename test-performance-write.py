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

data_schema_performance_write = StructType([
    StructField("id", LongType(), True),
    StructField("foreclosure_date", StringType(), True),
])

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Assemble").getOrCreate()

    final_structure_performance = StructType(fields=data_schema_performance)
    lines_per = spark.read.option("sep","|").csv("data/Performance_*.txt",header="False", nullValue = "NA",schema=final_structure_performance)
    
    performance = lines_per.select("id","foreclosure_date").filter("foreclosure_date IS NOT NULL")
    performance.coalesce(1).write.option("header","true").parquet("Performance/")

    #SparkSession.stop()