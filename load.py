from dotenv import load_dotenv
from sqlalchemy import create_engine


def generate_load_pwd():
    load_dotenv()
    return os.getenv('POSTGRESQL_PASS')

def generate_engine()
    pwd = generate_load_pwd()
    url_prefix = 'postgresql://factright:'
    url_suffix = '@database-2.ch19a6fwwbwn.us-east-2.rds.amazonaws.com:5432/FR_Dashboard'
    return url_prefix + pwd + url_suffix

def load(data):
    engine = create_engine(generate(engine))
    data.to_sql('Temp Hubspot Deals', engine)
    upsert = 'INSERT INTO "Hubspot Deals"
       (index,
        portalId,
        dealId,
        properties_dealname_value,
        properties_hubspot_owner_id_value,
        properties_dealstage_value,
        properties_hs_object_id_value,
        properties_createdate_value,
        properties_amount_value,
        properties_dealtype_value,
        propertie_closedate_value) AS A
    SELECT
        index,
        portalId,
        dealId,
        properties_dealname_value,
        properties_hubspot_owner_id_value,
        properties_dealstage_value,
        properties_hs_object_id_value,
        properties_createdate_value,
        properties_amount_value,
        properties_dealtype_value,
        properties_closedate_value
    FROM
        "Temp Hubspot Deals" AS B
    ON CONFLICT (dealId) DO UPDATE SET
    index = B.index,
    portalId = B.portalId,
    dealId = B.dealId,
    properties_dealname_value = B.properties_dealname_value,
    properties_hubspot_owner_id_value = B.properties_hubspot_owner_id_value,
    properties_dealstage_value = B.properties_dealstage_value,
    properties_hs_object_id_value = B.properties_hs_object_id_value,
    properties_createdate_value = B.properties_createdate_value,
    properties_amount_value = B.properties_amount_value,
    properties_dealtype_value = B.properties_dealtype_value,
    properties_closedate_value = B.properties_closedate_value;'
    connection.execute(upsert)
    drop = 'DROP TABLE "Temp Hubspot Deals";'
    connection.execute(drop)
