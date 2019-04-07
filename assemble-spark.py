"""
1. Combine all the Performance files into one file
2. Combine all the Acquisition files into one file

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

#When Loading IntegerType, the schema returns null
#Need Type Conversion for types other than String

spark = SparkSession.builder.appName("Assemble").getOrCreate()

data_schema_acquisition = [
StructField("id",IntegerType(),True),
StructField("channel",StringType(),False),
StructField("seller",StringType(),False),
StructField("interest_rate",StringType(),False),
StructField("balance",StringType(),False),
StructField("loan_term",StringType(),False),
StructField("origination_date",StringType(),False),
StructField("first_payment_date",StringType(),False),
StructField("ltv",StringType(),False),
StructField("cltv",StringType(),False),
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

final_structure_acquisition  = StructType(fields=data_schema_acquisition)

#df = spark.read.text("data/Acquisition_2012Q1.txt",schema=final_structure_performance,delimiter"|")
lines = spark.read.option("sep","|").csv("data/Acquisition_2012Q1.txt",header="False", nullValue = "NA", schema=final_structure_acquisition)

lines.show(10)

#data_schema_performance
#final_structure_performance