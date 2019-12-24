import json
import pandas as pd
from flatten_json import flatten

def clean_data(data):
    PARAMS = ['portalId',
              'dealId',
              'properties_dealname_value',
              'properties_hubspot_owner_id_value',
              'properties_dealstage_value',
              'properties_hs_object_id_value',
              'properties_createdate_value',
              'properties_amount_value',
              'properties_dealtype_value',
              'properties_closedate_value']
    df = pd.DataFrame.from_dict([flatten(d) for d in data])
    return df[PARAMS]
