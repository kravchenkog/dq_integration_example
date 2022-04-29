import os
from data_quality.dq_female_output import SalesDqFemaleOutput
from data_quality.dq_male_output import SalesDqMaleOutput
import pandas as pd
import sqlite3


class SalesDataPipeline:
    def __init__(self):
        """
        Example of the Data Pipeline based on the Pandas
        (do not judge strictly - I am not a date engineer :) )
        """
        self.path_to_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sales_pipeline.db")
        self.connector = sqlite3.connect(self.path_to_db)
        self.cursor = self.connector.cursor()

        self.output_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sales_csv")
        self.output_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sales_json")
        self.sales_data_frame = None

    def get_sales_separated_by_gender(self, gender: str) -> pd.DataFrame:
        self.sales_data_frame = pd.read_sql_query("SELECT * FROM sales_input_data", self.connector)
        df = self.sales_data_frame.loc[self.sales_data_frame['gender'] == gender]
        return df

    @staticmethod
    def get_sales_by_payment_method(payment_method: str, df: pd.DataFrame):
        df = df.loc[df['payment'] == payment_method]
        return df

    @staticmethod
    def get_sales_price_lower_then(price_lower: int, df: pd.DataFrame):
        df = df.loc[df['unit_price'] < price_lower]
        return df

    @staticmethod
    def get_sales_quantity_lower_then(quantity_lower: int, df: pd.DataFrame):
        df = df.loc[df['quantity'] < quantity_lower]
        return df

    def save_output(self, output_data: pd.DataFrame, output_table: str):
        self.save_output_to_db(
            table_name=output_table,
            df=output_data
        )
        output_data.to_json(
            os.path.join(self.output_json_path, f"{output_table}.json"),
            indent=3
        )
        output_data.to_csv(
            path_or_buf=os.path.join(self.output_csv_path, f"{output_table}.csv")
        )

    def save_output_to_db(self, table_name: str, df: pd.DataFrame):
        self.cursor.executemany(f'insert into {table_name} '
                                f'(invoice_id, branch, city, customer_type, gender, product_line, unit_price, quantity,'
                                f'tax, total, date, time, payment, cogs, gross_margin_percentage, gross_income, rating)'
                                f'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);',
                                df.to_records(index=False))
        self.connector.commit()

    def data_quality_checks_sales_pipeline(self, df: pd.DataFrame, table_name: str):
        if table_name == "female_output":
            SalesDqFemaleOutput(
                female_output_df=df,
                connector=self.connector,
                cursor=self.cursor,
                table_name=table_name
            )
        if table_name == "male_output":
            SalesDqMaleOutput(
                male_output_df=df,
                connector=self.connector,
                cursor=self.cursor,
                table_name=table_name
            )


if __name__ == '__main__':
    """
    Pipeline steps:
    1.
    2.
    3.
    4.
    => data quality checks for first data frame
    5. save pipeline output for first data frame
    => data quality checks for second data frame
    6. save pipeline output for second data frame
    """
    sales = SalesDataPipeline()

    # pipeline step 1
    male_df = sales.get_sales_separated_by_gender(gender="Male")
    female_df = sales.get_sales_separated_by_gender(gender="Female")

    # pipeline step 2
    male_df = sales.get_sales_by_payment_method(df=male_df, payment_method="Credit card")
    female_df = sales.get_sales_by_payment_method(df=female_df, payment_method="Credit card")

    # pipeline step 3
    male_df = sales.get_sales_price_lower_then(df=male_df, price_lower=50)
    female_df = sales.get_sales_price_lower_then(df=female_df, price_lower=50)

    # pipeline step 4
    male_df = sales.get_sales_quantity_lower_then(df=male_df, quantity_lower=3)
    female_df = sales.get_sales_quantity_lower_then(df=female_df, quantity_lower=3)

    # DATA QUALITY - Data Frame 1 (for male df)
    sales.data_quality_checks_sales_pipeline(
        df=male_df,
        table_name="male_output"
    )

    # pipeline save output 1
    sales.save_output(output_data=male_df, output_table="sales_output_male")

    # DATA QUALITY - Data Frame 2 (for female df)
    sales.data_quality_checks_sales_pipeline(
        df=female_df,
        table_name="female_output"
    )

    # pipeline save output 2
    sales.save_output(output_data=female_df, output_table="sales_output_female")
