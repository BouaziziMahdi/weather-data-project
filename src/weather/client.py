import requests


BASE_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather() :
    params = {
        "latitude": 36.8065,
        "longitude": 10.1815,
        "current": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
            "weather_code",
        ]),
        "timezone": "Africa/Tunis",
    }

    reponse= requests.get(
        BASE_URL, 
        params=params,
        timeout=10
    )

    reponse.raise_for_status()
    return reponse.json()
