# Import
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F

from requests_weather import get_weather_forecast

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Read CSV file into DataFrame
json_weather = get_weather_forecast()

def get_four_day_forecast_dataframe(json_weather = json_weather):
    """Transforms the 
    Args:
        json_weather: Dictionary of json out

     
    
    Returns:
    ------------------------
    
    
    """
    dataframe = spark.createDataFrame(json_weather['items'][0]['forecasts']) 
    pivot_df = dataframe.select(F.col('date'), F.col('forecast'), F.explode(F.col('temperature')).alias('temperature_low_high', 'temperature')) \
            .groupBy("date", 'forecast').pivot("temperature_low_high").sum('temperature')

    return pivot_df


if __name__ == '__main__':
    get_four_day_forecast_dataframe()