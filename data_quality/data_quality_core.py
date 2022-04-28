from great_expectations import dataset
import warnings
from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd
from typing import List, Dict
from datetime import datetime
import json
from dataclasses import dataclass
import os
import sqlite3


@dataclass
class DqException:
    exception_message: str
    is_error: bool = False


class DataQuality:

    def __init__(self, df, connector=None, cursor=None, table_name=None):
        self.df = df
        if type(self.df) == SparkDataFrame:
            self.df_ge = dataset.SparkDFDataset(self.df)
        elif type(self.df) == pd.DataFrame:
            self.df_ge = dataset.PandasDataset(self.df)
        else:
            return
        self.run_time = datetime.now()
        self.dq_report_fields = [
            'success',
            'expectation_type',
            'kwargs',
            'description',
            'ts',
            'is_error',
            'table_name'
        ]
        self.dq_report = pd.DataFrame(columns=self.dq_report_fields)
        self.connector = self.get_connector() if not connector else connector
        self.cursor = self.connector.cursor() if not cursor else cursor
        self.table_name = table_name

    def dq_finalize(self):
        warnings_df: pd.DataFrame = self.dq_report.loc[(self.dq_report['success'].astype(str).str.contains('False')) &
                                                       (self.dq_report['is_error'].astype(str).str.contains('False'))]
        errors_df: pd.DataFrame = self.dq_report.loc[(self.dq_report['success'].astype(str).str.contains('False')) &
                                                     (self.dq_report['is_error'].astype(str).str.contains('True'))]
        if warnings_df.shape[0]:
            warnings.warn(
                '\033[33m' + f"\n{warnings_df.to_json(indent=3, orient='records', lines=True)}" + '\033[m')
        if errors_df.shape[0]:
            raise Exception("'\033[91m'" + f"\n{errors_df.to_json(indent=3, orient='records', lines=True)}" + '\033[m')

    def expect_column_to_exist(self, column_name, exception: DqException):
        if type(self.df_ge) is not dataset.PandasDataset and type(self.df_ge) is not dataset.SparkDFDataset:
            warnings.warn('\033[33m' + "\nThis expectation is not supported of your dataframe" + '\033[m')
            return

        result = self.df_ge.expect_column_to_exist(column_name, result_format='SUMMARY')
        self.add_result_to_report(result, exception)

    def expect_column_values_distinct_to_be_in_set(self, column: str, values_li: List, exception: DqException):
        if type(self.df_ge) is not dataset.PandasDataset and type(self.df_ge) is not dataset.SparkDFDataset:
            warnings.warn('\033[33m' + "\nThis expectation is not supported of your dataframe" + '\033[m')
            return

        result = self.df_ge.expect_column_distinct_values_to_be_in_set(
            column,
            values_li
        )
        self.add_result_to_report(result, exception)

    def add_result_to_report(self, result, dq_exception: DqException):
        record: Dict = {
            "success": result.success,
            "expectation_type": result.expectation_config.expectation_type,
            "kwargs": json.dumps(result.expectation_config.kwargs),
            "description": dq_exception.exception_message,
            "ts": self.run_time,
            "is_error": dq_exception.is_error,
            "table_name": self.table_name
        }

        # add result to dataframe
        df_record = pd.DataFrame(record, index=[0])
        self.dq_report = pd.concat([self.dq_report, df_record], ignore_index=True)

        # add result to DB
        self.add_record_to_db(record, table_name="dq_report")

    def add_record_to_db(self, records, table_name):
        records['ts'] = str(records['ts'])
        df = pd.DataFrame(records, index=[0])

        # convert bool
        mask = df.applymap(type) != bool
        d = {True: 'TRUE', False: 'FALSE'}

        df = df.where(mask, df.replace(d))

        # insert to dq DB
        self.cursor.executemany(f'insert into {table_name} '
                                f'({", ".join(self.dq_report_fields)})'
                                f'values (?,?,?,?,?,?,?);',
                                df.to_records(index=False))
        self.connector.commit()

    @staticmethod
    def get_connector():
        path_to_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "sales_pipeline.db")
        return sqlite3.connect(path_to_db)
