# Import
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F

from requests_weather import get_weather_forecast

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Read CSV file into DataFrame

def get_four_day_forecast_dataframe(json_weather):
    """Transforms the json to a DataFrame with [ date|forecast|high|low]
    Args:
        json_weather: Dictionary of json output from get_weather_forecast()

    Returns:
        pivot_df: pyspark.DataFrame
    """
    dataframe = spark.createDataFrame(json_weather['items'][0]['forecasts'])
    pivot_df = dataframe.select(F.col('date'), F.col('forecast'), F.explode(F.col('temperature')).alias('temperature_low_high', 'temperature')) \
            .groupBy("date", 'forecast').pivot("temperature_low_high").sum('temperature')

    return pivot_df


if __name__ == '__main__':
    weather_4_day_json = get_weather_forecast()
    four_day_forecast_df = get_four_day_forecast_dataframe(weather_4_day_json)