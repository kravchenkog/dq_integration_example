from data_quality.data_quality_core import DataQuality, DqException
import os
import sqlite3


class SalesDqFemaleOutput:

    def __init__(self, female_output_df, connector=None, cursor=None, table_name=None):
        self.female_output_df = female_output_df
        self.connector = connector
        self.cursor = cursor
        self.dq_exception = DqException
        self.dq = DataQuality(
            df=female_output_df,
            connector=connector,
            cursor=cursor,
            table_name=table_name
        )

        self.run_all()
        self.dq.dq_finalize()

    def run_all(self):
        self.dq.expect_column_to_exist(
            "payment",
            exception=DqException(
                exception_message="Expect 'payment' column is in list of columns TC345654",
                is_error=False
            )
        )
        self.dq.expect_column_to_exist(
            "unit_price",
            exception=DqException(
                exception_message="Expect 'unit_price' column is in list of columns TC343654",
            )
        )
        self.dq.expect_column_to_exist(
            "unit_price",
            exception=DqException(
                exception_message="Expect 'unit_price_w' column is in list of columns TC343654",
            )
        )
        self.dq.expect_column_values_distinct_to_be_in_set(
            column="gender", values_li=["Female"],
            exception=DqException(
                exception_message="Expect only 'Female' values is in values only in the gender Column TC343654 ",
                is_error=False
            )
        )
        self.dq.expect_column_to_exist(
            column_name="blabla",
            exception=DqException(
                exception_message="Expect 'blabla' column is in list of columns TC345654",
                is_error=False
            )
        )
