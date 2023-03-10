import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('Empty Spark').getOrCreate()

#create empty RDD

emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

# creating  empty RDD using parallelize
rdd2 = spark.sparkContext.parallelize([])

#createing schema structure
schema = StructType([
    StructField('firstName', StringType(), True),
    StructField('middleName', StringType(), True),
    StructField('lastName', StringType(), True)
    ]
)

#creating empty dataframe using emptyrdd and schema
emptydf = spark.createDataFrame(emptyRDD, schema)
emptydf.printSchema()

df1 = emptyRDD.toDF(schema)
df1.printSchema()

df2 = spark.createDataFrame([],schema)
df2.printSchema()

df3 = spark.createDataFrame([], StructType())
df3.printSchema()
