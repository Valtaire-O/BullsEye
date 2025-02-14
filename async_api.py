from collections import Counter
import asyncio
import aiohttp
from typing import Optional, List,Dict
import logging
from db_connect import CloudConnect
import time
from tenacity import retry, stop_after_attempt, stop_after_delay
class Asnyc_API():
    """Make asynchronous post and get requests to any endpoint"""
    def __init__(self):
        self.response_map = Counter()
        self.cloud_conn =CloudConnect()

    @retry(stop=(stop_after_delay(50) | stop_after_attempt(3)))
    async def post(self, session, data, endpoint, headers):
        try:
               body = data['body']
               async with session.post(endpoint, headers=headers, json=body, ssl=False) as r:
                   text = await r.json()
                   logging.info(f"Context Post Status: {r.status}")
                   return {"status": r.status, 'response': text}
        except Exception as error:
               logging.info(f"Context Post Status:  503 {error}")
               return {"status": 503, 'response': None}

    @retry(stop=(stop_after_delay(50) | stop_after_attempt(3)))
    async def fetch(self, session, data, endpoint):
        try:
            async with session.get(endpoint, ssl=False) as r:
                logging.info(f"Scraping Status: {r.status}")
                self.response_map[endpoint] = {"status": r.status}
                html = await r.json()
                return {'html': html['result']['content'], 'status': r.status}
        except Exception as error:
            logging.info(f"Scraping Status: {503}")
            return {'html': '', 'status': 503}

    @retry(stop=(stop_after_delay(50) | stop_after_attempt(3)))
    async def fetch_img(self,session,data, endpoint):

        try:
            async with session.get(endpoint, ssl=False) as r:
                content = await r.content.read()
                content_type = r.content_type
                logging.info(f"First Contact: Img Link Status: {r.status}")
                return  {"status":r.status, 'response':f"{content}",'content_type':content_type}
        except Exception as error:

            logging.info(f"Img Link Status: 500")

            return {"status": 503, 'response': None,'content_type':None}


    async def handle_all(self, session, data,  headers, scrape_state, image_state, api_state):
        tasks = []

        for d in data:

            if api_state is True:

                endpoint = d['model_endpoint']
                task = asyncio.create_task(self.post(session, d, endpoint, headers))
            elif scrape_state is True:

                endpoint = d['endpoint']

                task = asyncio.create_task(self.fetch(session, d, endpoint))
            else:
                endpoint = d['endpoint']
                task = asyncio.create_task(self.fetch_img(session, d, endpoint))

            if task:
                tasks.append(task)

        res = await asyncio.gather(*tasks)
        return res

    async def start_async(self,data,headers,scrape_state=False,image_state=False,api_state=False):

        # set task dependant timeouts
        if api_state:

            sock_connect = 100
            sock_read = 100
        else:
            sock_connect = 50
            sock_read = 50

        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=sock_connect, sock_read=sock_read)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=50,limit_per_host=50) ,timeout=session_timeout) as session:
            html = await self.handle_all(session, data, headers, scrape_state, image_state, api_state)
            i = -1
            for d in data:

                i += 1
                if scrape_state is True:
                    d['html'] = html[i]['html']
                    d['status'] = html[i]["status"]
                elif api_state is True :
                    d['response'] =  html[i]["response"]
                    d['status'] = html[i]["status"]
                elif image_state is True:
                    d['response'] = html[i]["response"]
                    d['status'] = html[i]["status"]
                    d['content_type'] = html[i]["content_type"]
        return data

    def make_calls(self,data:List[Dict],headers,scrape_state=False,image_state=False,api_state=False):
        """Expecting the key endpoint/model_endpoint within the list of dicts
        returns the same data set with keys: response, html, content etc"""
        #confirm exactly one state has been provided
        check_state = [i for i in [scrape_state, image_state, api_state] if i == True]
        if len(check_state) !=1 :
            raise AssertionError

        req_data = []
        count = 0
        '''Batch up dataset for async requests'''
        batch_data = self.make_batch(data,50)
        n = len(batch_data)
        for dim in batch_data:
            count += 1
            logging.info(f'Request for batch... {count} out of {n} batches')
            extract = asyncio.run(Asnyc_API().start_async(dim, headers=headers,
                    scrape_state=scrape_state,image_state=image_state,api_state=api_state) )
            req_data.extend(extract)
            if count >=2:
                time.sleep(1)
        return req_data

    def make_batch(self,arr, batch_size):
        """make a matrix from a flat list"""
        i = 0
        j = batch_size
        all_data = []
        n = len(arr)
        if batch_size > n:
            return [arr]
        while j <= n:
            if j == n:
                j += 1

            all_data.append(arr[i:j])
            remaining = len(arr[j:])
            if 0 < remaining < batch_size:
                i += batch_size
                j += remaining
                batch_size = remaining
                continue

            i += batch_size
            j += batch_size
        return all_data

    def api_key(self):
        key = '&key=' + self.cloud_conn.get_scraping_api_key()
        return key

    def api_params(self):
        params = '&country=us&render_js=true&rendering_wait=2500&proxy_pool=public_residential_pool'
        return params

    def api_url(self):
        return 'https://api.scrapfly.io/scrape?url='


