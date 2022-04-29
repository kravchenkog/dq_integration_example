from pyspark.sql import SparkSession
import pyspark
from data_quality.dq_female_output import SalesDqFemaleOutput
from data_quality.dq_male_output import SalesDqMaleOutput
import os


class SalesDataPipeline:
    def __init__(self):
        """
        Example of the Data Pipeline based on the Pandas
        (do not judge strictly - I am not a date engineer :) )
        """
        self.path_to_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sales_pipeline.db")
        self.output_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sales_csv")
        self.output_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sales_json")

        self.scSpark = SparkSession.builder \
            .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0') \
            .getOrCreate()
        self.sdf_data = self.scSpark.read.format('jdbc') \
            .options(
            url=f'jdbc:sqlite:{self.path_to_db}',
            dbtable='sales_input_data',
            driver='org.sqlite.JDBC') \
            .option("customSchema", "date STRING").load()
        self.sdf_data.registerTempTable("sales_male")
        self.sdf_data.registerTempTable("sales_female")

    def get_sales_separated_by_gender(self):
        return self.scSpark.sql(f"select * from sales_male where gender == 'Male'"), \
               self.scSpark.sql(f"select * from sales_female where gender == 'Female'")

    def get_sales_by_payment_method(self, payment_method, df):
        df.createOrReplaceTempView("payment_method")
        return SparkSession.sql(self=self.scSpark,
                                sqlQuery=f"Select * from payment_method where payment == '{payment_method}'")

    def get_sales_price_lower_then(self, price_lower: int, df):
        df.createOrReplaceTempView("price")
        return SparkSession.sql(
            self.scSpark,
            sqlQuery=f'SELECT * from price WHERE `unit_price` < {price_lower}'
        )

    def get_sales_quantity_lower_then(self, quantity_lower: int, df):
        df.createOrReplaceTempView("quantity_view")
        return SparkSession.sql(
            self.scSpark,
            sqlQuery=f'SELECT * from quantity_view WHERE `quantity` < {quantity_lower}'
        )

    def data_quality_checks_sales_pipeline(self, df, table_name):
        if table_name == "female_output":
            SalesDqFemaleOutput(
                female_output_df=df,
                table_name=table_name
            )
        if table_name == "male_output":
            SalesDqMaleOutput(
                male_output_df=df,
                table_name=table_name
            )

    def save_output(self, output_data, table_name):
        output_data.write.format('jdbc') \
            .options(
            url=f'jdbc:sqlite:{self.path_to_db}',
            dbtable=table_name,
            driver='org.sqlite.JDBC').mode("overwrite").save()


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
    male_df, female_df = sales.get_sales_separated_by_gender()

    # pipeline step 2
    male_df = sales.get_sales_by_payment_method(
        payment_method="Credit card",
        df=male_df
    )
    female_df = sales.get_sales_by_payment_method(
        payment_method="Credit card",
        df=female_df)

    # pipeline step 3
    male_df = sales.get_sales_price_lower_then(
        price_lower=50,
        df=male_df
    )
    female_df = sales.get_sales_price_lower_then(
        price_lower=50,
        df=female_df
    )

    # pipeline step 4
    output_male = sales.get_sales_quantity_lower_then(
        df=male_df,
        quantity_lower=3
    )
    output_female = sales.get_sales_quantity_lower_then(
        df=female_df,
        quantity_lower=3
    )

    # DATA QUALITY 1
    sales.data_quality_checks_sales_pipeline(
        df=output_male,
        table_name="male_output"
    )

    # 5. pipeline save output 1
    sales.save_output(
        output_data=output_male,
        table_name='sales_output_male'
    )

    # DATA QUALITY 2
    sales.data_quality_checks_sales_pipeline(
        df=output_female,
        table_name="female_output"
    )

    # 6. pipeline save output 2
    sales.save_output(
        output_data=output_male,
        table_name='sales_output_female'
    )
