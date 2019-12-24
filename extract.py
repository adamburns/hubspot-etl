import json
import os
from dotenv import load_dotenv
from urllib.parse import urlencode
from urllib.request import urlopen
from time import sleep


def load_key():
    load_dotenv()
    return ('hapikey', os.getenv('HUBSPOT_API_KEY'))


def build_url(url_prefix, params, offset):
    if offset:
        params = [(val, key) for (val, key) in params if val != 'offset']
        params.append(('offset', offset))
    return url_prefix + urlencode(params)


def download_data(url):
    #TODO: Implement sleeping before getting  all of the data to prevent DDOS
    with urlopen(url) as response:
        data = json.loads(response.read())
        return data


def extract(hasMore = True):
    params = [('includeAssociations', 'true'), 
              ('limit', '250'), 
              ('properties', 'dealname'),
              ('properties', 'dealstage'),
              ('properties', 'createdate'), 
              ('properties', 'closedate'),
              ('properties', 'hubspot_owner_id'),
              ('properties', 'hs_object_id'), 
              ('properties', 'amount'),
              ('properties', 'dealtype')
    ]
    params.append(load_key())
    url_prefix = "https://api.hubapi.com/deals/v1/deal/paged?"
    data = []
    offset = None
    while(hasMore):
        temp_data = download_data(build_url(url_prefix, params, offset))
        data += temp_data['deals']
        hasMore = temp_data['hasMore']
        offset = temp_data['offset']
        sleep(1)
    return data
