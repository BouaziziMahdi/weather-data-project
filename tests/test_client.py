from unittest.mock import Mock

import pytest
import requests

from src.weather import client


def test_fetch_weather_returns_payload(monkeypatch):
    payload = {"current": {"temperature_2m": 25}}
    response = Mock()
    response.json.return_value = payload
    get = Mock(return_value=response)
    monkeypatch.setattr(client.requests, "get", get)

    assert client.fetch_weather() == payload
    response.raise_for_status.assert_called_once_with()
    args, kwargs = get.call_args
    assert args == (client.BASE_URL,)
    assert kwargs["timeout"] == 10
    assert kwargs["params"]["latitude"] == 36.8065
    assert kwargs["params"]["longitude"] == 10.1815
    assert kwargs["params"]["timezone"] == "Africa/Tunis"
    assert set(kwargs["params"]["current"].split(",")) == {
        "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "weather_code"
    }


@pytest.mark.parametrize("error", [requests.Timeout, requests.ConnectionError])
def test_network_failure_propagates(monkeypatch, error):
    monkeypatch.setattr(client.requests, "get", Mock(side_effect=error("unavailable")))
    with pytest.raises(error):
        client.fetch_weather()


def test_http_failure_does_not_parse_body(monkeypatch):
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("503")
    monkeypatch.setattr(client.requests, "get", Mock(return_value=response))
    with pytest.raises(requests.HTTPError):
        client.fetch_weather()
    response.json.assert_not_called()


def test_invalid_json_propagates(monkeypatch):
    response = Mock()
    response.json.side_effect = requests.exceptions.JSONDecodeError("invalid", "x", 0)
    monkeypatch.setattr(client.requests, "get", Mock(return_value=response))
    with pytest.raises(requests.exceptions.JSONDecodeError):
        client.fetch_weather()
