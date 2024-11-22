from prefect.blocks.system import Secret
from prefect import flow, task
from datetime import datetime, timedelta
import calendar

import pandas as pd
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

@task(log_prints=True)
def get_toast_access_token(toastAPIHost):
    
    secret_block = Secret.load("analytics-creds")

    creds = secret_block.get()

    ToastUserId = creds['toastAnalyticsAPIUserProd']
    ToastSecret = creds['toastAnalyticsAPISecretProd']

    auth_url = f"{toastAPIHost}/authentication/v1/authentication/login"

    headers = {
        "Content-Type": "application/json"
    }

    payload = {
        "clientId": ToastUserId,
        "clientSecret": ToastSecret,
        "userAccessType": "TOAST_MACHINE_CLIENT"
    }

    response = requests.post(auth_url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()

    toastAccessToken = response.json()['token']['accessToken']
    toastAccessStatus = response.json()['status']
  
    print("Authentication Status: "+toastAccessStatus)

    return toastAccessToken

@flow(log_prints=True)
def create_sales_summary(extract_mode: str, api_range_start_hardcoded: str, relative_days: int, request_sleep_time: int):
    print("---------Setting Orchestration Paramenters---------")
    api_range_start, api_range_end = set_orchestration_parameters(extract_mode, api_range_start_hardcoded, relative_days)

    print("---------Authenticating to Toast---------")
    toastAPIHost = 'https://ws-api.toasttab.com'
    toastAccessToken = get_toast_access_token(toastAPIHost)

    print("---------Setting Orchestration Paramenters---------")

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