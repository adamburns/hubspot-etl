import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def generate_load_pwd():
    load_dotenv()
    return os.getenv('POSTGRESQL_PASS')


def generate_engine():
    pwd = generate_load_pwd()
    url_prefix = 'postgresql://factright:'
    url_suffix = '@database-2.ch19a6fwwbwn.us-east-2.rds.amazonaws.com:5432/Dashboard'
    return url_prefix + pwd + url_suffix


def load(data):
    engine = create_engine(generate_engine())
    data.to_sql('Hubspot Deals', engine, if_exists='replace')
