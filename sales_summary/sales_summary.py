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

@task(log_prints=True)
def post_report_request(toastAPIHost, toastAccessToken, report_start_date, Locations):
    report_request_url = f"{toastAPIHost}/era/v1/menu/day"

    headers = {
        "Authorization": f"Bearer {toastAccessToken}",
        "Content-Type": "application/json"
    }

    payload = {
          "restaurantIds": [Locations[0], Locations[1], Locations[2]],
          "excludedRestaurantIds": [],
          "startBusinessDate": report_start_date,
          "groupBy": ["MENU"]
    }

    response = requests.post(report_request_url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()

    reportGUID = response.json()

    return reportGUID

@task(log_prints=True)
def get_report_results(toastAPIHost, toastAccessToken, reportGUID):
    report_retrieve_url = f"{toastAPIHost}/era/v1/menu/{reportGUID}"

    headers = {
        "Authorization": f"Bearer {toastAccessToken}",
        "Content-Type": "application/json"
    }

    response = requests.get(report_retrieve_url, headers=headers)
    response.raise_for_status()

    return response.json()

@flow(log_prints=True)
def create_sales_summary(extract_mode: str, api_range_start_hardcoded: str, relative_days: int, request_sleep_time: int):

    #Location Details
    Location_SahadiSpirits = "03ab87dc-a410-414f-b6f6-edacdbed27ad"
    Location_SahadiIC = "962f0037-3c2a-4126-84d6-9d18ee7e1c35"
    Location_SahadiAA = "fbddfd44-5ab0-4761-95ef-a1bd1c6996f5"
    Locations = [Location_SahadiSpirits, Location_SahadiIC, Location_SahadiAA]
    LocationNames = ["Sahadi Spirits", "Sahadi's - Industry City", "Sahadi's - Atlantic Avenue"]

    print("---------Setting Orchestration Parameters---------")
    api_range_start, api_range_end = set_orchestration_parameters(extract_mode, api_range_start_hardcoded, relative_days)

    print("---------Authenticating to Toast---------")
    toastAPIHost = 'https://ws-api.toasttab.com'
    toastAccessToken = get_toast_access_token(toastAPIHost)

    print("---------Pull New Reporting Data---------")
    report_date = api_range_start
    while report_date <= api_range_end:
        print("Requesting: " + report_date)
        reportGUID = post_report_request(toastAPIHost, toastAccessToken, report_date, Locations)
        time.sleep(request_sleep_time)

        print("Retrieving: " + report_date + " with GUID " + reportGUID)
        daily_date_json = get_report_results(toastAPIHost, toastAccessToken, reportGUID)

        print("Exporting: " + report_date)
        print(daily_date_json)
        #save_daily_data(daily_date_json, WorkingPath, CustomerName + "_Daily_" + report_date)

        report_date = (datetime.strptime(report_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
    print("Reporting requests complete!")

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