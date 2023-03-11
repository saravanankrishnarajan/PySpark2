import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType, IntegerType, ArrayType, MapType

spark = SparkSession.builder.appName("change Schema Structure").getOrCreate()

data = [("James", "", "Smith", "36636", "M", 3000),
       ("Michael", "Rose", "", "40288", "M", 4000),
       ("Robert", "", "Williams", "42114", "M", 4000),
       ("Maria", "Anne", "Jones", "39192", "F", 4000),
       ("Jen", "Mary", "Brown", "", "F", -1)
       ]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
    ])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)
from pyspark.sql.functions import col,struct,when
updatedDF = df.withColumn("OtherInfo",
                       struct(col("id").alias('identifier'),
                       col("gender").alias("gender"),
                       col("salary").alias("salary"),
                       when(col("salary").cast(IntegerType())<2000,"Low")
                           .when(col("salary").cast(IntegerType())<4000,"Medium")
                           .otherwise("High").alias("Salary_Grade")
                       )).drop("id","gender","salary")
updatedDF.printSchema()
updatedDF.show(truncate=False)


