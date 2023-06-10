from filelock import FileLock
import time
import json
from time import perf_counter
from scrapfly.scrapy import  ScrapflyScrapyRequest,ScrapflyCrawlSpider
from scrapfly import ScrapeConfig
from bulls_eye.shared_classes import validations
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import re
from collections import Counter
import os
from scrapy.signalmanager import dispatcher
from scrapy.signalmanager import SignalManager
from scrapy import signals

'''class GetHtml(ScrapflyCrawlSpider):
    def __init__(self, *a, **kw):

        super().__init__(*a, **kw)
        SignalManager(dispatcher.Any).connect(
            self.start_requests, signal=signals.spider_closed)
        self._compile_rules()

    #handle_httpstatus_list = [i for i in range(300,600)]
    name = 'findimage'
    spider_path = 'bulls_eye/bulls_eye/spiders'
    image_folder = 'image_data'
    site_folder = 'site_data'
    image_abs_path =  os.path.abspath(f"{spider_path}/{image_folder}")
    site_abs_path = os.path.abspath(f"{spider_path}/{site_folder}")

    blocked_or_empty_path = f"{site_folder}/blocked_or_empty.json"
    check_images_path = f"{site_folder}/check_images.json"
    none_found_path = f"{image_folder}/none_found.json"
    found_path = f"{image_folder}/found.json"
    lockfile = f"{site_folder}/lockfile.json"
    if __name__ != '__main__':
        lockfile = f"{site_abs_path}/lockfile.json"
        blocked_or_empty_path = f"{site_abs_path}/blocked_or_empty.json"
        check_images_path = f"{site_abs_path}/check_images.json"
        none_found_path = f"{image_abs_path}/none_found.json"
        found_path = f"{image_abs_path}/found.json"


    with open(check_images_path) as f:
        data = json.load(f)["Row"]
    with open(blocked_or_empty_path) as f:
        none_found = json.load(f)["Row"]

    used_backup = False

    record_holder, stored_content = Counter(), Counter()
    stored_status, seen,already_found  = Counter(), Counter(),Counter()

    queue,found = [],[]
    unique_links = []
    req_count = 0

    for d in data:
        url = d["image"]
        if record_holder[url] ==0:
            record_holder[url] = [d]
            unique_links.append({"image":url})
            continue

        record_holder[url].append(d)
    n = len(unique_links)




    #if data:
    def start_requests(self):
                yield from self.req_details(GetHtml.unique_links)
    def memoize_record(self,url,record):
        image = None
        if GetHtml.record_holder[url] == 0:
            GetHtml.record_holder[url] = [record]
            image = url
            #print(BullsEye.unique_links)
        else:
           GetHtml.record_holder[url].append(record)
        return image
    def req_details(self, data):
        for i in data:
            url = i["image"]
            if  GetHtml.seen[url] ==0:
                yield ScrapflyScrapyRequest(
                            scrape_config=ScrapeConfig(url=url, rendering_wait=5000, retry=False, render_js=True, country='us'),
                            callback=self.parse,errback=self.handle_error, meta={'download_timeout': 15}
                        )
                GetHtml.seen[url] = 1

            elif GetHtml.seen[url] == 1:
                # in the case of multiple records sharing possible images,
                # theres no need to make more requests
                yield  from self.stored_parse(url)

    def handle_error(self, failure):
        """Handle critical Failures within generator obj"""

        status = re.compile(r'[0-9]{3,4}').findall(str(failure.value))
        content = 'text/html'

        if status:
            status = status[0]
        else:
            status = 404
        print("sent To err back")
        GetHtml.stored_content[failure.request.url] = content
        GetHtml.stored_status[failure.request.url] = status

        response_data = {"status": status, "content": content, "request_url": failure.request.url}
        self._record_check(origin='Errback', url=failure.request.url)
        return self.decipher_image(response_data)



    def stored_parse(self,url):
        """Get saved data on urls that have already been requested"""
        status = GetHtml.stored_status[url]
        content = GetHtml.stored_content[url]
        response_data = {"status":status, "content":content, "request_url":url}
        self._record_check(origin='Stored Parse', url=url)
        #print(f"Stored Parse Count on :  {url}\n")

        return self.decipher_image(response_data)

    def parse(self, response):
        """For first time requests with a good status code"""
        #if BullsEye.seen[response.request.url] == 0:
            # protect against retries messing up the count!
        status = response.status
        content = str(response.headers.get('content-type'))
        GetHtml.stored_content[response.request.url] = content
        GetHtml.stored_status[response.request.url] = status
        print(response.request.url)

        response_data = {"status": status, "content": content, "request_url": response.request.url}
        self._record_check(origin='Parse', url=response.request.url)
        return self.decipher_image(response_data)

    def _record_check(self, url, origin):
        try:
            assert GetHtml.record_holder[url] != 0
        except:
            print(f'{origin}: Failure to access Record through request url{url}')
            data = {"fail_origin": origin, "req_url":url}
            currently_held = []
            for q in GetHtml.record_holder:
                print('Records: ' + q, GetHtml.record_holder[q][0]['image'], '\n')
                currently_held.append({"key":q,"value":GetHtml.record_holder[q]})
            data["currently_held"] = currently_held




    def decipher_image(self,response_data:dict):
        GetHtml.req_count += 1

        content =response_data["content"]
        status = response_data["status"]
        request_url = response_data["request_url"]
        asset_list = GetHtml.record_holder[request_url]
        if asset_list != 0:
            for asset in asset_list:
                asset['content_type'] = content
                asset['img_response'] = status

                if not asset['content_type']:
                    print(f"No content type:{request_url}  {content}")
                    GetHtml.none_found.append(asset)

                elif status == 404 or 'image' not in content:
                    print('Bad:',response_data["request_url"],status,'\n')

                    if len(asset["possible_images"]) >1:
                        # add the record to the queue, this time with a new image
                        asset["possible_images"].pop(0)
                        asset["image"] = asset["possible_images"][0]
                        GetHtml.queue.append(asset)
                    else:
                        GetHtml.none_found.append(asset)
                else:

                    GetHtml.found.append(asset)
                    #print('good:', asset["image"], asset['content_type'], asset['img_response'], '\n')

        print(f"Request Count:{GetHtml.req_count}  N:{GetHtml.n}")


        if  GetHtml.req_count==GetHtml.n and GetHtml.queue:
            time.sleep(15)
            print(f'Found = {len(GetHtml.found)}')
            print(f'trying again with {len(GetHtml.queue)} records')
            # reset base case
            next_round = GetHtml.queue
            GetHtml.queue = []
            GetHtml.record_holder = Counter()
            GetHtml.req_count = 0
            GetHtml.unique_image = []
            for row in next_round:
                url = row["image"]
                image_link = self.memoize_record(url, row)
                if image_link:
                    GetHtml.unique_image.append(image_link)
                
            # make call with new dataset

            GetHtml.n = len(GetHtml.unique_image)
            yield from self.req_details(GetHtml.unique_image)

        elif GetHtml.queue == [] and GetHtml.req_count == GetHtml.n and GetHtml.used_backup is False:
                GetHtml.used_backup = True
                print('Trying clear_bit backup')

                validate = validations()
                clear_bit = 'https://logo.clearbit.com/'
                last_round = GetHtml.none_found
                GetHtml.none_found = []

                GetHtml.req_count = 0
                GetHtml.record_holder = Counter()

                GetHtml.unique_image = []
                for row in last_round:
                    main_url = row['url']
                    row["image"] = clear_bit+ validate.get_base_url(main_url)
                    url = row["image"]
                    row['possible_images'] = []
                    image_link = self.memoize_record(url,row)
                    if image_link:
                        GetHtml.unique_image.append(image_link)
                  
                GetHtml.n = len(GetHtml.unique_image)

                yield from self.req_details(GetHtml.unique_image)


        elif GetHtml.queue == [] and GetHtml.req_count==GetHtml.n:
            print("Finished")
            js = open(GetHtml.found_path, "w")
            js.write(json.dumps({"Row": GetHtml.found}, indent=2, sort_keys=True))
            js.close()

            js = open(GetHtml.none_found_path, "w")
            js.write(json.dumps({"Row": GetHtml.none_found}, indent=2, sort_keys=True))
            js.close()'''

'''if __name__ == '__main__':
    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(GetHtml)
    start = perf_counter()

    process.start()
    stop = perf_counter()
    print("time taken:", stop - start)'''