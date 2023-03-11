import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Convert PySpark DF to Pandas").getOrCreate()

columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")]
df = spark.createDataFrame(data,columns)
df.show()

df.show(truncate=False)

df.show(2, truncate=False)

df.show(2,truncate=25)

df.show(2,truncate=25,vertical=True)