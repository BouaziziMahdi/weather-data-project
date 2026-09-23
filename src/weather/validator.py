def validation_weather_data(data: dict) -> bool:
    if not isinstance(data, dict) or not data:
        return False
    current_weather = data.get("current")
    if not isinstance(current_weather, dict) or not current_weather:
        return False
    required_keys = [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "weather_code",
    ]   

    return all(
        key in current_weather for key in required_keys
    )
