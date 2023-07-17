import requests
from bs4 import BeautifulSoup
import io


import base64
import json

import pandas as pd
import urllib.request, urllib.parse, urllib.error
import json
from collections import Counter

data = [{'dc_id': '1671030779208x349526006795827000', 'mainURL': 'https://hallecompanies.com/', 'name': 'The Halle Companies', 'status': 'Needs Classification', 'logo': 'https://hallecompanies.com/img-sys/IP_changed.png'}]
clear_bit = 'https://logo.clearbit.com/'
url= 'https://www.minorityinnovationweekend.org/wp-content/uploads/2019/05/shutterstock_408585073_resized.jpg'

'https://www.townebank.com/townebank/media/business/small%20business%20owners/open-graph-fb-brewery-owner.jpg'
'''response = requests.get(f'{url}')
x =response.headers.get('content-type')

print(x,response.status_code)'''


def bubble_paginator(url, table, headers):
    params = {'cursor': 0,
              'limit': 100}
    results = []
    #print(f'Trying: {url} {table}')
    response = requests.get(url, headers=headers, params=params).json()
    print(response)
    raw_response = response['response']['results']
    try:
        left = response['response']['remaining']
    except:
        print(f'{url} has no records here')
        return

    pages = round(left / 100)
    if pages < (left / 100):
        pages += 1
    print(f'{left} assets left in  {pages} pages')

    for response_record in raw_response:
        results.append(response_record)

    for record_count in range(pages):
        cursor = 100 * (
                record_count + 1)  # moving the data up per page? left value is curser, right value is page placement?
        params = {'count': 100,
                  'cursor': cursor}
        response = requests.get(url, headers=headers, params=params).json()
        raw_response = response['response']['results']

        for response_record in raw_response:
            results.append(response_record)

    js = open("staging_data.json", "w")
    js.write(json.dumps({"Row": results}, indent=2, sort_keys=True))
    js.close()

    print(len(results))
    temp_df = pd.DataFrame(results)
    return temp_df

def dci_get( ):
    table = 'Unified%20Assets'
    '''constraints = json.dumps([{"key": "originEcosystem",
                                   "constraint_type": "equals",
                                   "value": "1676055772048x691967172136681900"},
                                {"key": "Int_Status",
                                   "constraint_type": "equals",
                                   "value": "Needs Review"}
                                                         ])'''

    constraints = json.dumps([{"key":"belongsInEcosystem",
                                   "constraint_type": "contains",
                                   "value": f"1678305895726x743536852024950800"}])

    url = f'https://data.ecomap.tech/api/1.1/obj/{table}/?constraints={constraints}'


    headers = {
        'Authorization': 'Bearer 1b850b046f18ae20ed7116b62c3ddeec',
        'Content-Type': 'application/json'
    }

    return bubble_paginator(url, table, headers)

dci_get()
#https://www.mcccmd.com/business-coalition.html
