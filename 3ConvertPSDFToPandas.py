import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Convert PySpark DF to Pandas").getOrCreate()

data = [("James", "", "Smith","36636","M", 600000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]

# Creatomg spark Dataframe
pysparkDF = spark.createDataFrame(data=data, schema=columns)
pysparkDF.printSchema()
pysparkDF.show(truncate=False)

# Create pandas DataFrame from Spark DataFrame

pandasDF = pysparkDF.toPandas()
print(pandasDF.head())

pysparkDF2 = spark.createDataFrame(pandasDF)

rdd2 = pysparkDF2.rdd
print(rdd2.collect())

rdd3 = pysparkDF2.rdd.map(tuple)
print(rdd3.collect())

rdd4 = pysparkDF2.rdd.map(list)
print(rdd2.collect())


# Nested StructType
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

dataStruct = [(("James","","Smith"),"36636","M","3000"), \
      (("Michael","Rose",""),"40288","M","4000"), \
      (("Robert","","Williams"),"42114","M","4000"), \
      (("Maria","Anne","Jones"),"39192","F","4000"), \
      (("Jen","Mary","Brown"),"","F","-1") \
]


schemaStruct = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
          StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', StringType(), True)
         ])

nestedDF = spark.createDataFrame(data=dataStruct, schema = schemaStruct)
nestedDF.printSchema()
nestedDF.show()