import allure
import os
import sqlite3
import pandas as pd


@allure.story("Sales Pipeline test")
class TestSalesPipelineDataQuality:

    @allure.title("DQ Sales Pipeline WARNING: {dq_result_warning.description}")
    def test_warnings(self, dq_result_warning):
        with allure.step("WARNING results"):
            allure.attach(dq_result_warning.to_csv(),
                          attachment_type=allure.attachment_type.CSV,
                          name=f"error")

        assert dq_result_warning.success

    @allure.title("DQ Sales Pipeline ERROR: {dq_result_error.description}")
    def test_errors(self, dq_result_error: pd.Series):
        with allure.step("ERROR results"):
            allure.attach(dq_result_error.to_csv(),
                          attachment_type=allure.attachment_type.CSV,
                          name=f"warning")

        assert dq_result_error.success


def pytest_generate_tests(metafunc):
    # connect to DB
    path_to_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sales_pipeline.db")
    connector = sqlite3.connect(path_to_db)
    dq_report_full = pd.read_sql_query("SELECT * FROM dq_report", connector, index_col=None)

    # parse booleans
    values = {'TRUE': True, 'FALSE': False}
    dq_report_full = dq_report_full.replace(values)

    # get last run
    last_pipeline_run_results: pd.DataFrame = dq_report_full.loc[dq_report_full['ts'] == dq_report_full['ts'].max()]
    last_pipeline_run_results.reset_index(inplace=True, drop=True)

    # get tuple of series from df
    params_li_warnings = []
    params_li_errors = []
    for index, row in last_pipeline_run_results.iterrows():
        if row.is_error:
            params_li_errors.append(row)
        else:
            params_li_warnings.append(row)

    # parametrize for warnings test
    if "dq_result_warning" in metafunc.fixturenames:
        metafunc.parametrize("dq_result_warning",
                             params_li_warnings,
                             scope='session',
                             ids=[e[3] for e in params_li_warnings])

    # parametrize for errors test
    if "dq_result_error" in metafunc.fixturenames:
        metafunc.parametrize("dq_result_error",
                             params_li_errors,
                             scope='session',
                             ids=[e[3] for e in params_li_errors])
