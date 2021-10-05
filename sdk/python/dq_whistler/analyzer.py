import json
import numpy as np
from typing import Dict, List, Any, Union
import pyspark.sql.functions as F
from pandas.core.frame import DataFrame as pandas_df
from pyspark.sql.dataframe import DataFrame as spark_df
from dq_whistler.profiler.string_profiler import StringProfiler
from dq_whistler.profiler.number_profiler import NumberProfiler


class NpEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, np.integer):
			return int(obj)
		if isinstance(obj, np.floating):
			return float(obj)
		if isinstance(obj, np.ndarray):
			return obj.tolist()
		return super(NpEncoder, self).default(obj)


class DataQualityAnalyzer:
	"""
	Analyzer class responsible for taking :obj:`JSON` dict and executing it on the columnar data

	Args:
			data (:obj:`pyspark.sql.DataFrame` | :obj:`pandas.core.series.Series`): Dataframe/Series containing the data
			config (:obj:`List[Dict[str, str]]`): The array of dicts containing config for each column
	"""
	_data: Union[spark_df, pandas_df]
	_config: List[Dict[str, str]]

	def __init__(self, data: Union[spark_df, pandas_df], config: List[Dict[str, str]]):
		"""
		Creates an instance of DQAnalyzer
		"""
		self._data = data
		self._config = config

	def analyze(self) -> str:
		"""
		Returns:
			:obj:`str`: :obj:`JSON` string containing stats for multiple columns
		"""
		final_checks: List[Dict[str, Any]] = []
		# TODO: Add feature of automatic column detection, if config is not present
		for column_config in self._config:
			# TODO:: checks for key existence
			column_name = column_config.get("name")
			column_data_type = column_config.get("datatype")

			if isinstance(self._data, spark_df):
				column_data = self._data.select(F.col(column_name))

			if isinstance(self._data, pandas_df):
				column_data = self._data[column_name]

			if column_data_type == "string":
				profiler = StringProfiler(column_data, column_config)
			elif column_data_type == "number":
				profiler = NumberProfiler(column_data, column_config)
			else:
				raise NotImplementedError
			output = profiler.run()
			final_checks.append({
				"col_name": column_name,
				**output
			})
		return json.dumps(final_checks, cls=NpEncoder)
