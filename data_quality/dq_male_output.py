import pandas as pd
import inspect
from data_quality.data_quality_core import DataQuality, DqException
from pyspark.sql import DataFrame as SparkDataFrame
from types import SimpleNamespace


class SalesDqMaleOutput:
    def __init__(self, male_output_df, connector=None, cursor=None, table_name=None):
        """
        This class and all expectations inside are related to male_output_df Data Frame only
        :param male_output_df: the data frame for which expectations should be applied
        :param connector: connector to DB
        :param cursor: cursor for DB
        :param table_name: the name of the table for the DQ report
        """
        self.male_output_df = male_output_df
        self.connector = connector
        self.cursor = cursor

        self.dq_exception = DqException
        self.dq = DataQuality(
            df=self.male_output_df,
            connector=self.connector,
            cursor=self.cursor,
            table_name=table_name
        )

        # run all custom expectations
        self.run_all_custom()

        # run all core (standard) expectations
        self.run_all_core()

        # finalize pipeline (raise exception or warnings if exists)
        self.dq.dq_finalize()

    def run_all_custom(self):
        """
        custom expectations test suite
        :return: None
        """
        self.check_male_from_city_unit_price_not_more(
            city="Mandalay",
            unit_price_threshold=10,
            exception=DqException(
                exception_message="Expect that 'unit_price' more then 10 if buyer from 'Mandalay' city and is Male"
            )
        )

    def run_all_core(self):
        """
        core expectations test suite
        :return: None
        """
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
            column="gender", values_li=["Male"],
            exception=DqException(
                exception_message="Expect only 'Male' values is in values only in the gender Column TC343654 ",
                is_error=False
            )
        )
        self.dq.expect_column_to_exist(
            column_name="blabla male",
            exception=DqException(
                exception_message="Expect 'blabla' column is in list of columns TC345654",
                is_error=False
            )
        )

    def check_male_from_city_unit_price_not_more(self, city: str, unit_price_threshold: float, exception: DqException):
        """
        this function is created as custom dq check (expectation) example which related to current DataFrame only
        works for Pandas and Spark Data Frame ...

        :param city: city name
        :param unit_price_threshold: unit price threshold
        :param exception: dq expectation
        :return: None
        """
        # check Data Frame type
        if type(self.male_output_df) is pd.DataFrame:
            # get count unexpected rows
            count_of_unexpected: int = (self.male_output_df.loc[
                (self.male_output_df['city'] == city) & (self.male_output_df['unit_price'] < unit_price_threshold)]) \
                .shape[0]

        # check Data Frame type
        elif type(self.male_output_df) is SparkDataFrame:
            # get count unexpected rows
            count_of_unexpected: int = self.male_output_df.filter(
                f'city="{city}" and unit_price < {unit_price_threshold}') \
                .count()

        else:
            return
        # create DQ result (the same format as ExpectationValidationResult from ge library)
        dq_result = SimpleNamespace(
            success=False if count_of_unexpected else True,
            expectation_config=SimpleNamespace(
                expectation_type=inspect.stack()[1][4][0],  # current function name
                kwargs={'city': city,
                        'unit_price_threshold': unit_price_threshold}),

        )
        # add result Data Quality report DB
        self.dq.add_result_to_report(
            result=dq_result,
            dq_exception=exception
        )
