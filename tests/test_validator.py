from src.weather.validator import validation_weather_data
import pytest


@pytest.mark.parametrize("data", [None, [], "bad", 42, {}, {"current": None},
                                      {"current": []}, {"current": "bad"}])
def test_invalid_payload_shapes(data):
    assert validation_weather_data(data) is False


@pytest.mark.parametrize("missing", ["temperature_2m", "relative_humidity_2m",
                                    "wind_speed_10m", "weather_code"])
def test_each_required_field(missing):
    current = dict(temperature_2m=0, relative_humidity_2m=0,
                   wind_speed_10m=0, weather_code=0)
    del current[missing]
    assert validation_weather_data({"current": current}) is False


def test_zero_values_are_valid():
    assert validation_weather_data({"current": dict(
        temperature_2m=0, relative_humidity_2m=0, wind_speed_10m=0, weather_code=0
    )}) is True

def test_weather_validations():
    data={
        "current": {
            "temperature_2m": 25.0,
            "relative_humidity_2m": 60,
            "wind_speed_10m": 5.0,
            "weather_code": 1,
            "time": "2024-06-01T12:00:00Z"
        }
    }
    assert validation_weather_data(data) is True

def test_weather_data_empty():
    assert validation_weather_data({}) is False

def test_missing_temperature():
    data={
        "current": {
            "relative_humidity_2m": 60,
            "wind_speed_10m": 5.0,
            "weather_code": 1,
            "time": "2024-06-01T12:00:00Z"
        }
    }
    assert validation_weather_data(data) is False
