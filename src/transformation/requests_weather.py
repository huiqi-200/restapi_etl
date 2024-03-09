import requests

# The API endpoint
url = "https://api.data.gov.sg/v1/environment/2-hour-weather-forecast"



# A POST request to tthe API
post_response = requests.get(url)

# Print the response
post_response_json = post_response.json()
print(post_response_json)

"""
{'userID': 1, 'id': 101, 'title': 'Making a POST request', 'body': 'This is the data we created.'}
"""

def get_weather_forecast(url = "https://api.data.gov.sg/v1/environment/2-hour-weather-forecast"):
    post_response = requests.get(url)

    # Print the response
    post_response_json = post_response.json()
    return post_response_json


if __name__ == "__main__":
    get_weather_forecast()