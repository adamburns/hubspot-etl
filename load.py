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
    data.to_sql('Temp Hubspot Deals', engine, if_exists='replace')
    connection = engine.connect()
    upsert = """
INSERT INTO "Hubspot Deals"
	("index",
	"portalId",
	"dealId",
	properties_dealname_value,
	properties_hubspot_owner_id_value,
	properties_dealstage_value,
	properties_hs_object_id_value,
	properties_createdate_value,
	properties_amount_value,
	properties_dealtype_value,
	properties_closedate_value)
SELECT
	"index",
	"portalId",
	"dealId",
	properties_dealname_value,
	properties_hubspot_owner_id_value,
	properties_dealstage_value,
	properties_hs_object_id_value,
	properties_createdate_value,
	properties_amount_value,
	properties_dealtype_value,
	properties_closedate_value
FROM
	"Temp Hubspot Deals"
ON CONFLICT ("dealId") DO UPDATE SET
	"index" = EXCLUDED."index",
    "portalId" = EXCLUDED."portalId",
    "dealId" = EXCLUDED."dealId",
    properties_dealname_value = EXCLUDED.properties_dealname_value,
    properties_hubspot_owner_id_value = EXCLUDED.properties_hubspot_owner_id_value,
    properties_dealstage_value = EXCLUDED.properties_dealstage_value,
    properties_hs_object_id_value = EXCLUDED.properties_hs_object_id_value,
    properties_createdate_value = EXCLUDED.properties_createdate_value,
    properties_amount_value = EXCLUDED.properties_amount_value,
    properties_dealtype_value = EXCLUDED.properties_dealtype_value,
    properties_closedate_value = EXCLUDED.properties_closedate_value;
    """
    connection.execute(upsert)
    drop = 'DROP TABLE \"Temp Hubspot Deals\";'
    connection.execute(drop)
