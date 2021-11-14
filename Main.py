from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pathlib

spark_context = SparkContext()
spark_con = SparkSession.builder.getOrCreate()

df = spark_con.read.csv(
    str(pathlib.Path(pathlib.Path(_file_).parent.parent, 'titanic.csv')), header=True)

df.groupby('name').toPandas()