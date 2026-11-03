"""
from pyspark.sql import *
from pyspark.sql.types import StructType, StringType, StructField ,FloatType,IntegerType , DoubleType
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("flattened_nested_data").master("local[*]").getOrCreate()

data=[("amit",29),("anshu",19),("sumit",28)]

schema=StructType([StructField("name",StringType(),True),
                  StructField("age",IntegerType(),True)])

df=spark.createDataFrame(data,schema)
df.show()

spark.stop()
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = (
	SparkSession.builder.appName("flatten_nested_data")
	.master("local[*]")
	.config("spark.driver.host", "127.0.0.1")
	.config("spark.driver.bindAddress", "127.0.0.1")
	.getOrCreate()
)

data = [
	("Asha", 25, ["reading", "hiking"], ("Jaipur", "302001")),
	("Ravi", 30, ["music"], ("Delhi", "110001")),
]

schema = "name STRING, age INT, hobbies ARRAY<STRING>, address STRUCT<city: STRING, zip: STRING>"
df = spark.createDataFrame(data, schema)

flattened_df = df.select(
	col("name"),
	col("age"),
	explode(col("hobbies")).alias("hobby"),
	col("address.city").alias("city"),
	col("address.zip").alias("zip"),
)

flattened_df.show(truncate=False)


from input import EMPLOYEE_FILE ,MANAGERS_FILE

employees_df=spark.read.format("csv").option("header","true").option("inferSchema","true").csv(str(EMPLOYEE_FILE))
employees_df.show()

managers_df=spark.read.format("csv").option("header","true").option("inferSchema","true").csv(str(MANAGERS_FILE))

managers_df.show()

spark.stop()


 