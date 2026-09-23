"""Exercise real Spark transformations without Kafka, Delta jars, or PostgreSQL."""
from datetime import datetime
import json
import unittest

from pyspark.sql import SparkSession
from src.spark.transformations import clean_weather, latest_observations, aggregate_hourly


class SparkTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("WeatherCITests")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "Africa/Tunis")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def bronze(self, payloads):
        return self.spark.createDataFrame([
            (json.dumps(payload), "weather", 0, offset, datetime(2026, 9, 22, 11, 0))
            for offset, payload in enumerate(payloads)
        ], "raw_json string, topic string, partition int, offset long, kafka_timestamp timestamp")

    def payload(self, **changes):
        current = dict(time="2026-09-22T12:15", temperature_2m=20.0,
                       relative_humidity_2m=60, wind_speed_10m=4.0, weather_code=1)
        current.update(changes)
        return dict(latitude=36.8, longitude=10.1, timezone="Africa/Tunis",
                    elevation=10.0, current=current)

    def test_silver_filters_invalid_observations(self):
        rows = [self.payload(), self.payload(relative_humidity_2m=-1),
                self.payload(relative_humidity_2m=101), self.payload(wind_speed_10m=-1.0),
                self.payload(temperature_2m=None), self.payload(time=None), {}]
        clean = clean_weather(self.bronze(rows)).collect()
        self.assertEqual(len(clean), 1)
        self.assertEqual(clean[0].temperature_2m, 20.0)
        self.assertEqual(clean[0].offset, 0)

    def test_zero_values_and_humidity_bounds(self):
        rows = [self.payload(temperature_2m=0.0, relative_humidity_2m=0, wind_speed_10m=0.0),
                self.payload(relative_humidity_2m=100)]
        self.assertEqual(clean_weather(self.bronze(rows)).count(), 2)

    def test_latest_event_wins_and_locations_remain_separate(self):
        other = self.payload()
        other["latitude"] = 40.0
        clean = clean_weather(self.bronze([
            self.payload(temperature_2m=10.0), self.payload(temperature_2m=30.0), other
        ]))
        result = latest_observations(clean).collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(next(row for row in result if row.latitude == 36.8).temperature_2m, 30.0)

    def test_hourly_metrics_and_time_buckets(self):
        clean = clean_weather(self.bronze([
            self.payload(temperature_2m=10.0, wind_speed_10m=2.0, relative_humidity_2m=40),
            self.payload(time="2026-09-22T12:45", temperature_2m=30.0,
                         wind_speed_10m=6.0, relative_humidity_2m=80),
            self.payload(time="2026-09-22T13:00"),
        ]))
        rows = aggregate_hourly(clean).orderBy("hour").collect()
        self.assertEqual(len(rows), 2)
        first = rows[0]
        self.assertEqual(first.observation_count, 2)
        self.assertEqual((first.avg_temperature, first.min_temperature, first.max_temperature),
                         (20.0, 10.0, 30.0))
        self.assertEqual((first.avg_humidity, first.avg_wind_speed, first.max_wind_speed),
                         (60.0, 4.0, 6.0))
        self.assertEqual(rows[1].observation_count, 1)
        hours = aggregate_hourly(clean).selectExpr("date_format(hour, 'HH:mm') as hour").collect()
        self.assertEqual({row.hour for row in hours}, {"12:00", "13:00"})


if __name__ == "__main__":
    unittest.main()
