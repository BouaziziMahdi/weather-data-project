import unittest
from unittest.mock import patch

from airflow.models import DagBag


class WeatherDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder="/opt/airflow/dags")
        cls.dag = cls.bag.dags.get("weather_ingestion")

    def test_import_and_dependencies(self):
        self.assertEqual(self.bag.import_errors, {})
        self.assertIsNotNone(self.dag)
        chain = ["fetch_validate_and_publish", "bronze", "silver", "gold", "gold_to_postgres"]
        self.assertEqual(set(self.dag.task_ids), set(chain))
        for index, name in enumerate(chain):
            expected = {chain[index + 1]} if index + 1 < len(chain) else set()
            self.assertEqual(self.dag.get_task(name).downstream_task_ids, expected)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_ingestion_success_and_failures(self):
        function = self.dag.get_task("fetch_validate_and_publish").python_callable
        namespace = function.__globals__
        good = {"current": dict(temperature_2m=25, relative_humidity_2m=60,
                                wind_speed_10m=5, weather_code=1)}
        for payload, error in [(good, None), ({}, ValueError), (None, ValueError)]:
            with self.subTest(payload=payload):
                with patch.dict(namespace), patch(
                    function.__module__ + ".fetch_weather", return_value=payload
                ), patch(function.__module__ + ".publish_weather") as publish:
                    if error:
                        with self.assertRaises(error):
                            function()
                        publish.assert_not_called()
                    else:
                        function()
                        publish.assert_called_once_with(good)

    def test_spark_scripts_exist(self):
        from pathlib import Path
        for name in ["bronze", "silver", "gold", "gold_to_postgres"]:
            task = self.dag.get_task(name)
            self.assertTrue(Path(task.application).is_file(), task.application)
            self.assertEqual(task._conn_id, "spark_default")


if __name__ == "__main__":
    unittest.main()
