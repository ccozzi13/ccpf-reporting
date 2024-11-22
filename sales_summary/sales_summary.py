from prefect import flow, task
from datetime import datetime, timedelta
import calendar

#import pandas as pd
import glob
import re

import requests
import json
import time

@task(log_prints=True)
def set_orchestration_parameters(extract_mode: str, api_range_start_hardcoded: str, relative_days: int):

    today = datetime.now()

    api_range_start_relative = (today - timedelta(days=relative_days)).strftime("%Y%m%d")
    api_range_end = today.strftime("%Y%m%d")

    print("Selected Mode: "+extract_mode)
    if extract_mode == "Hardcoded":
        print("Range Start: "+api_range_start_hardcoded)
        print("Range Start: "+api_range_end)
        api_range_start = api_range_start_hardcoded
    elif extract_mode == "Relative":
        print("Range Start: "+api_range_start_relative)
        print("Range End: "+api_range_end)
        api_range_start = api_range_start_relative
    
    return api_range_start, api_range_end

@flow
def create_sales_summary(extract_mode: str, api_range_start_hardcoded: str, relative_days: int, request_sleep_time: int):
    api_range_start, api_range_end = set_orchestration_parameters(extract_mode, api_range_start_hardcoded, relative_days)
    print(api_range_start)
    print(api_range_end)

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/ccozzi13/ccpf-reporting.git", 
        entrypoint="sales_summary/sales_summary.py:create_sales_summary"
    ).deploy(
        name="sales-summary-reporting", 
        work_pool_name="TestPool", 
        parameters={"extract_mode": "Relative", "api_range_start_hardcoded": "20240101", "relative_days": 1, "request_sleep_time": 8},
        job_variables={"pip_packages": ["pandas"]}
    )