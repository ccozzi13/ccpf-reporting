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
def say_hello(name: str):
    print(f"Hello, {name}!")

@flow
def hello_universe(names: list[str]):
    for name in names:
        say_hello(name)

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/ccozzi13/ccpf-reporting.git", 
        entrypoint="testing/hello_world.py:hello_universe"
    ).deploy(
        name="my-first-deployment", 
        work_pool_name="TestPool", 
    )