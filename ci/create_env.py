import base64
import os
import secrets

values = {
    "AIRFLOW_UID": str(os.getuid()),
    "AIRFLOW_IMAGE_NAME": "weather-airflow:ci",
    "FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
    "AIRFLOW__API_AUTH__JWT_SECRET": secrets.token_hex(32),
    "WEATHER_DB_HOST": "weather-postgres",
    "WEATHER_DB_PORT": "5432",
    "WEATHER_DB_NAME": "weather_ci",
    "WEATHER_DB_USER": "weather_ci",
    "WEATHER_DB_PASSWORD": secrets.token_hex(24),
    "MAIL_USERNAME": "ci@example.invalid",
    "MAIL_PASSWORD": "unused",
}
with open(".env", "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as output:
    output.write("".join(f"{key}={value}\n" for key, value in values.items()))
