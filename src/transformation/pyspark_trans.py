# Import
from pyspark.sql import SparkSession
from pyspark.sql import Row

from requests_weather import get_weather_forecast

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Read CSV file into DataFrame
json_weather = get_weather_forecast()

rdd = spark.sparkContext.parallelize(json_weather)
df = spark.read.json(rdd)


print(df)
