# Converting PySParkRDD to DataFrame

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Convert RDD to DataFrame").getOrCreate()

dept =[("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
print(rdd.collect())


# Without predefined schema
df = rdd.toDF()
df.printSchema()
df.show(truncate=False)


# create dataframe with predefined columns
deptColumns = ["dept_name","dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)
df



deptDF = spark.createDataFrame(rdd,schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# create structure of schema and making all as stringtype
from pyspark.sql.types import StructType, StructField, StringType

deptSchema = StructType([
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(),True)]
)
deptDF1 = spark.createDataFrame(rdd, schema=deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)