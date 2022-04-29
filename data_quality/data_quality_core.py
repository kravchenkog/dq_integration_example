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
        """
        The class is created as example for core functionality of Data Quality checks
        :param df: DataFrame (Spark or Pandas)
        :param connector: connector to DB (to save DQ report)
        :param cursor: cursor to DB (to save DQ report)
        :param table_name: the name of the table for the DQ report
        """
        self.df = df
        self.df_ge = self.get_df_ge(df)
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

    @staticmethod
    def get_df_ge(df):
        """
        The function returns great expectation data frame depends on incoming dataFrame type
        :param df: dataFrame (spark or pandas)
        :return: great_expectations data frame (dataset.SparkDFDataset or dataset.PandasDataset)
        """
        if type(df) == SparkDataFrame:
            return dataset.SparkDFDataset(df)
        elif type(df) == pd.DataFrame:
            return dataset.PandasDataset(df)
        else:
            return

    def dq_finalize(self):
        """
        finalize the Data Pipeline (Raise errors if exist and all warnings)
        :return: None
        """

        # get all warnings
        warnings_df: pd.DataFrame = self.dq_report.loc[
            (self.dq_report['success'].astype(str).str.contains('False')) &
            (self.dq_report['is_error'].astype(str).str.contains('False'))
            ]

        # get all errors
        errors_df: pd.DataFrame = self.dq_report.loc[
            (self.dq_report['success'].astype(str).str.contains('False')) &
            (self.dq_report['is_error'].astype(str).str.contains('True'))
            ]

        # raise warnings if exist
        if warnings_df.shape[0]:
            warnings.warn(
                '\033[33m' + f"\n{warnings_df.to_json(indent=3, orient='records', lines=True)}" + '\033[m')

        # raise errors if exist
        if errors_df.shape[0]:
            raise Exception("'\033[91m'" + f"\n{errors_df.to_json(indent=3, orient='records', lines=True)}" + '\033[m')

    def expect_column_to_exist(self, column_name, exception: DqException):
        """
        The wrapper over core great_expectation core function expect_column_to_exist
        :param column_name: the name of the column to perform checks
        :param exception: the exception type (error or warning) and message
        :return: none
        """

        # check greatExpectation DF Type
        if type(self.df_ge) is not dataset.PandasDataset and type(self.df_ge) is not dataset.SparkDFDataset:
            warnings.warn('\033[33m' + "\nThis expectation is not supported of your dataframe" + '\033[m')
            return

        # get result of expectation
        result = self.df_ge.expect_column_to_exist(column_name, result_format='SUMMARY')

        # save result to DB
        self.add_result_to_report(result, exception)

    def expect_column_values_distinct_to_be_in_set(self, column: str, values_li: List, exception: DqException):
        """
        The wrapper over core great_expectation core function expect_column_distinct_values_to_be_in_set
        :param column: the name of the column to perform checks
        :param values_li: the list of values to check in the column
        :param exception: the exception type (error or warning) and message
        :return: None
        """

        # check greatExpectation DF Type
        if type(self.df_ge) is not dataset.PandasDataset and type(self.df_ge) is not dataset.SparkDFDataset:
            warnings.warn('\033[33m' + "\nThis expectation is not supported of your dataframe" + '\033[m')
            return

        # get result of expectation
        result = self.df_ge.expect_column_distinct_values_to_be_in_set(
            column,
            values_li
        )

        # save result to DB
        self.add_result_to_report(result, exception)

    def add_result_to_report(self, result, dq_exception: DqException):
        """
        Save Data quality result to DQ Data Base
        :param result: result of DQ check
        :param dq_exception: the exception type (error or warning) and message
        :return: None
        """

        # result to Dict
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
