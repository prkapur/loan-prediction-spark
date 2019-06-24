from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, VectorIndexer, OneHotEncoder, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import *


spark = SparkSession.builder.appName("Predict").getOrCreate()

df = spark.read.load("parquet/*.parquet", sep = ",", inferSchema="True")

df = df.withColumn("interest_rate_double", df["interest_rate"].cast(DoubleType()))

df = df.withColumn("balance_double", df["balance"].cast(DoubleType()))

df = df.withColumn("loan_term_int", df["loan_term"].cast(IntegerType()))

df = df.withColumn("ltv_int", df["ltv"].cast(IntegerType()))

df = df.withColumn("cltv_int", df["cltv"].cast(IntegerType()))

df = df.withColumn("borrower_count_int", df["borrower_count"].cast(IntegerType()))

df = df.withColumn("dti_int", df["dti"].cast(IntegerType()))

df = df.withColumn("borrower_credit_score_int", df["borrower_credit_score"].cast(IntegerType()))

df = df.withColumn("unit_count_int", df["unit_count"].cast(IntegerType()))

df = df.withColumn("zip_int", df["zip"].cast(IntegerType()))

df = df.withColumn("insurance_percentage_double", df["insurance_percentage"].cast(IntegerType()))

df = df.withColumn("foreclosure_status_int", df["foreclosure_status"].cast(IntegerType()))


channel_indexer = StringIndexer(inputCol = "channel", outputCol= "channel_index")
channel_encoder = OneHotEncoder(inputCol = "channel_index", outputCol = "channelVec")

seller_indexer = StringIndexer(inputCol = "seller", outputCol = "seller_index")
seller_encoder = OneHotEncoder(inputCol = "seller_index", outputCol = "sellerVec")

first_time_homebuyer_indexer = StringIndexer(inputCol = "first_time_homebuyer", outputCol = "first_time_homebuyer_index")
first_time_homebuyer_encoder = OneHotEncoder(inputCol = "first_time_homebuyer_index", outputCol = "firsttimehomebuyerVec")

loan_purpose_indexer = StringIndexer(inputCol = "loan_purpose", outputCol = "loan_purpose_index")
loan_purpose_encoder = OneHotEncoder(inputCol = "loan_purpose_index", outputCol = "loanpurposeVec")

property_type_indexer = StringIndexer(inputCol = "property_type", outputCol = "property_type_index")
property_type_encoder = OneHotEncoder(inputCol = "property_type_index", outputCol = "propertytypeVec")

occupancy_status_indexer = StringIndexer(inputCol = "occupancy_status", outputCol = "occupancy_status_index")
occupancy_status_encoder = OneHotEncoder(inputCol = "occupancy_status_index", outputCol = "occupancystatusVec")

property_state_indexer = StringIndexer(inputCol = "property_state", outputCol = "property_state_index")
property_state_encoder = OneHotEncoder(inputCol = "property_state_index", outputCol = "propertystateVec")

assembler = VectorAssembler(inputCols = ['channelVec','sellerVec','interest_rate_double','balance_double','loan_term_int',
                                         'ltv_int','cltv_int','borrower_count_int','dti_int','borrower_credit_score_int',
                                         'firsttimehomebuyerVec','loanpurposeVec','propertytypeVec',
                                         'unit_count_int', 'occupancystatusVec','propertystateVec',
                                         'zip_int','insurance_percentage_double'],outputCol = "features")

log_reg = LogisticRegression(featuresCol="features",labelCol='foreclosure_status_int')

pipeline = Pipeline(stages=[channel_indexer, seller_indexer, first_time_homebuyer_indexer, loan_purpose_indexer,
                            property_type_indexer, occupancy_status_indexer, property_state_indexer,
                            channel_encoder, seller_encoder, first_time_homebuyer_encoder, loan_purpose_encoder,
                            property_type_encoder,occupancy_status_encoder,property_state_encoder,
                            assembler, log_reg])

train_data, test_data = df.randomSplit([0.7,0.3])

fit_model = pipeline.fit(train_data)

results = fit_model.transform(test_data)

my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='foreclosure_status_int')

results.select('foreclosure_status_int','prediction').show()
