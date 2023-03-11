import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("Column Class").getOrCreate()
#colObj = lit("Column Class")
#data = [("James",23),("Ann",40)]
#
#df = spark.createDataFrame(data).toDF("name.fname","gender")
#df.printSchema()
#
## Different way of access
#df.cache()
#df.select(df.gender).show()
#df.select(df['gender']).show()
#df.select(df["`name.fname`"]).show()
#
##Using SQL col() function
#from pyspark.sql.functions import col
#df.select(col("gender")).show()
#df.select(col("`name.fname`")).show()

#from pyspark.sql import Row
#data2=[Row(name="James",prop=Row(hair="black",eye="blue")),
#      Row(name="Ann",prop=Row(hair="grey",eye="black"))]
#df2=spark.createDataFrame(data2)
#df2.printSchema()
#
#df2.cache()
#df2.show()
#
#df2.select(df2.prop.hair).show()
#df2.select(df2['prop.hair']).show()
#df2.select(col('prop.hair')).show()
#
#df2.select(col("prop.*")).show()

data=[(100,2,1),(200,3,4),(300,4,4)]
df3=spark.createDataFrame(data).toDF("col1","col2","col3")

df3.cache()
df3.show()

#arithmetic  operationgs
print("Addition")
df3.select(df3.col1 + df3.col2).show()
print("Subtraction")
df3.select(df3.col1 - df3.col2).show()
print("Multiply")
df3.select(df3.col1*df3.col2).show()
print("Division")
df3.select(df3.col1/df3.col2).show()
print("Modulus")
df3.select(df3.col1%df3.col2).show()
print("col2>col3")
df3.select(df3.col2,df3.col3, df3.col2>df3.col3).show()
print("col2 < col3")
df3.select(df3.col2,df3.col3,df3.col2<df3.col3).show()
print("col2 == col3")
df3.select(df3.col2,df3.col3,df3.col2 == df3.col3).show()