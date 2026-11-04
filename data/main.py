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





def flattened_nested(data,parent_key="",sep="."):
    flatten={}
    if isinstance(data,dict):
        for key, values in data.items():
            if parent_key:
                new_key=f"{parent_key}{sep}{key}"
            else:
                new_key=str(key)
            try:
                flatten.update(flattened_nested(values,new_key,sep))
            except Exception as error:
                raise ValueError(
                    f"could not flatten value at '{new_key}:{error}'"
				) from error
    elif isinstance(data,list):
        for index , item in enumerate(data):
            if parent_key:
                new_key=f"{parent_key}{sep}{index}"
            else:
                new_key=str(index)
            try:
                flatten.update(flattened_nested(item,new_key,sep))
            except Exception as error:
                raise ValueError(
                    f"could not flatten value at '{new_key}':{error}"
				) from error
    else:
        flatten[parent_key]=data
    return flatten

