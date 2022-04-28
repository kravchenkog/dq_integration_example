import pandas as pd
from great_expectations.expectations.expectation import ExpectationValidationResult
from data_quality.data_quality_core import DataQuality, DqException
from types import SimpleNamespace


class SalesDqMaleOutput:

    def __init__(self, male_output_df, connector=None, cursor=None, table_name=None):
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

        self.run_all_core()
        self.dq.dq_finalize()

    def run_all_custom(self):
        self.check_male_from_city_unit_price_more(
            city="Mandalay",
            unit_price_threshold=10,
            exception=DqException(
                exception_message="Expect that 'unit_price' more then 10 if buyer from 'Mandalay' city and is Male"
            )
        )

    def run_all_core(self):
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

    def check_male_from_city_unit_price_more(self, city: str, unit_price_threshold: float, exception: DqException):
        """
        this function is created as custom dq check (expectation) example which related to current DataFrame only
        works for Pandas DF only, for Spark will be added soon ...

        :param city: city name
        :param unit_price_threshold: unit price threshold
        :param exception: dq expectation
        :return: None
        """
        if type(self.male_output_df) is pd.DataFrame:
            df_result: pd.DataFrame = self.male_output_df.loc[
                (self.male_output_df['city'] == city) & (self.male_output_df['unit_price'] < unit_price_threshold)]

            dq_result = SimpleNamespace(
                success=False if df_result.shape[0] else True,
                expectation_config=SimpleNamespace(
                    expectation_type="custom expectation for male_output_df",
                    kwargs={'city': city,
                            'unit_price_threshold': unit_price_threshold}),

            )
            self.dq.add_result_to_report(
                result=dq_result,
                dq_exception=exception
            )
