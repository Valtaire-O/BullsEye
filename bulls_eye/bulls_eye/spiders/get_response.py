from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import time
import json
from scrapy.signalmanager import SignalManager
from time import perf_counter
from scrapfly.scrapy import  ScrapflyScrapyRequest,ScrapflyCrawlSpider
from scrapfly import ScrapeConfig
'''from extracts.scrape_queries import  ExtractQueries
from extracts.validation import ValidateInput'''
from  bulls_eye.shared_classes import validations
from bs4 import BeautifulSoup
from scrapy.signalmanager import dispatcher
from scrapy import signals
import re
from collections import Counter
import os


def update(d, other): d.update(other); return d
# Concatenates listing and product datasets with update
def remove_slash(value):
    return value.replace("/", " ").strip()
'''class BullsEye(ScrapflyCrawlSpider):
    """This class sets the class vars used in the scraping process"""



    handle_httpstatus_list = [i for i in range(300,600) ]
    name = 'test'

    site_folder = 'site_data'
    spider_path = 'bulls_eye/bulls_eye/spiders'
    site_abs_path = os.path.abspath(f"{spider_path}/{site_folder}")
    #Todo Improve this path toggle
    check_images_path = f"{site_folder}/check_images.json"
    start_set_path = f"{site_folder}/start_set.json"
    bad_request_path = f"{site_folder}/bad_request.json"
    blocked_or_empty_path = f"{site_folder}/blocked_or_empty.json"
    #lockfile = f"{site_folder}/lockfile.json"

    if __name__ != '__main__':
        #lockfile = f"{site_folder}/lockfile.json"
        check_images_path = f"{site_abs_path}/check_images.json"
        start_set_path = f"{site_abs_path}/start_set.json"

        bad_request_path = f"{site_abs_path}/bad_request.json"
        blocked_or_empty_path = f"{site_abs_path}/blocked_or_empty.json"

    with open(start_set_path) as f:
        data = json.load(f)["Row"]

    #all_urls = set(list([d["mainURL"] for d in data]))
    check_images,blocked_or_empty, bad_requests = [],[], []


    req_count = 0
    n = len(data)
  
    parent_path = '/Users/valentineokundaye/PycharmProjects/BullsEye'
    record_holder,already_seen = Counter(), Counter()
    link_validations = validations()
    for d in data:
        url = d["url"]
        record_holder[url] = d

    if data:
        def start_requests(self):
            yield from self.req_details(BullsEye.data)


    def req_details(self, data):
        count = 0
        for i in data:

            url = i["url"]

            yield ScrapflyScrapyRequest(
                scrape_config=ScrapeConfig(url=url, rendering_wait=5000,retry=False, render_js=True,  country='us'),
                callback=self.parse,errback=self.handle_error, meta={'download_timeout': 15}
                )

    def handle_error(self, failure):
        """Handle critical Failures within generator obj"""
        status = re.compile(r'[0-9]{3,4}').findall(str(failure.value))
        if status:
            status = status[0]
        else:
            status = 403
        print(status)
        response_data = {"status": status, "html": '', "request_url": failure.request.url}
        return self.resp_landing(response_data)



    def parse(self, response):
        status = response.status
        html = response.text
        response_data = {"status": status, "html": html, "request_url": response.request.url}
        return self.resp_landing(response_data)

    def resp_landing(self,response_data:dict):
        BullsEye.req_count += 1
        url = response_data["request_url"]
        status= response_data["status"]
        html = response_data["html"]

        asset = BullsEye.record_holder[url]
        if asset !=0:
            asset["response"] = status
            asset["request_url"] = url


            if not html:
                BullsEye.bad_requests.append(asset)

            elif status != 403 and status != 200:
                BullsEye.bad_requests.append(asset)

            elif status == 403:
                BullsEye.blocked_or_empty.append(asset)

            else:
                soup = BeautifulSoup(html, 'html.parser')
                #asset['html'] = html
                "Catch sites that return no images at all ( blocked Or PDF)"
                if not soup.find_all('img') and not soup.find_all('meta'):
                    BullsEye.blocked_or_empty.append(asset)
                else:
                    meta_block = f"{soup.find_all('meta')}"
                    image_block = f"{soup.find_all('img')}"


                    asset["meta_block"] = meta_block
                    asset["image_block"] = image_block

                    meta_soup = BeautifulSoup(meta_block, 'html.parser')
                    image_soup = BeautifulSoup(image_block, 'html.parser')
                    all_images = self.isolate_images(meta_soup, image_soup, url)

                    if all_images:
                        asset["image"] = all_images[0]
                        asset["possible_images"] = all_images

                        BullsEye.check_images.append(asset)
                    else:
                        BullsEye.blocked_or_empty.append(asset)

            print(f"Has: {len(BullsEye.check_images)} Hasnt: {len(BullsEye.blocked_or_empty+BullsEye.bad_requests)}\n")
            print(f"Count: {BullsEye.req_count} --> N:{BullsEye.n}")

        if BullsEye.req_count == BullsEye.n:
            js = open(BullsEye.blocked_or_empty_path, "w")
            js.write(json.dumps({"Row": BullsEye.blocked_or_empty}, indent=2, sort_keys=True))
            js.close()

            js = open(BullsEye.check_images_path, "w")
            js.write(json.dumps({"Row": BullsEye.check_images}, indent=2, sort_keys=True))
            js.close()
            js = open(BullsEye.bad_request_path, "w")
            js.write(json.dumps({"Row": BullsEye.bad_requests}, indent=2, sort_keys=True))
            js.close()





    def isolate_images(self,meta_block, image_block, main_url):
        """pick the best 10 images from the page"""
        already_seen = Counter()
        validate = validations()
        image_count = 0
        image_cap = 10

        meta_image = []
        logo_priority = []
        other_images = []

        for item in meta_block.find_all('meta'):
            link = item.get('property')
            if link and link == 'og:image':

                check_image = validate.image_file_type(validate.confirm_url(item.get('content'), main_url))
                if check_image:
                    image_count += 1
                    meta_image.append(check_image)
                    break

        for item in image_block.find_all('img'):
            src_link = item.get('src')
            if src_link:
                check_image = validate.image_file_type(validate.confirm_url(src_link, main_url))
                if check_image and already_seen[check_image] == 0:
                    logo_match = re.compile(rf'(?i)logo', flags=re.IGNORECASE).findall(check_image)
                    image_count += 1
                    if logo_match:
                        logo_priority.append(check_image)
                    else:
                        other_images.append(check_image)
                    already_seen[check_image] = 1
                if image_count == image_cap:
                    break
        all_images = meta_image + logo_priority + other_images

        return all_images'''


class BullsEye(ScrapflyCrawlSpider):
    """This class sets the class vars used in the scraping process"""

    #handle_httpstatus_list = [i for i in range(300, 600)]
    name = 'imagescraper'

    site_folder = 'site_data'
    image_folder = 'image_data'
    spider_path = 'bulls_eye/bulls_eye/spiders'
    site_abs_path = os.path.abspath(f"{spider_path}/{site_folder}")
    image_abs_path = os.path.abspath(f"{spider_path}/{image_folder}")
    # Todo Improve this path toggle
    #Save Initial Html Response
    check_images_path = f"{site_folder}/check_images.json"
    start_set_path = f"{site_folder}/start_set.json"
    bad_request_path = f"{site_folder}/bad_request.json"
    blocked_or_empty_path = f"{site_folder}/blocked_or_empty.json"
    #Image Finder
    none_found_path = f"{image_folder}/none_found.json"
    found_path = f"{image_folder}/found.json"
    # lockfile = f"{site_folder}/lockfile.json"
    initial_response_phase = True
    used_backup = False

    if __name__ != '__main__':
        # lockfile = f"{site_folder}/lockfile.json"
        check_images_path = f"{site_abs_path}/check_images.json"
        start_set_path = f"{site_abs_path}/start_set.json"
        bad_request_path = f"{site_abs_path}/bad_request.json"
        blocked_or_empty_path = f"{site_abs_path}/blocked_or_empty.json"

        none_found_path = f"{image_abs_path}/none_found.json"
        found_path = f"{image_abs_path}/found.json"

    with open(start_set_path) as f:
        data = json.load(f)["Row"]


    check_images, blocked_or_empty, bad_requests = [], [], []
    found,none_found, unique_links,queue = [], [], [], []
    req_count = 0
    n = len(data)

    parent_path = '/Users/valentineokundaye/PycharmProjects/BullsEye'

    record_holder, already_seen, already_requested = Counter(), Counter(),Counter()
    stored_content, stored_status,stored_html = Counter(), Counter(), Counter()
    link_validations = validations()

    for d in data:
        # memoize the initial data set to the key of unique links
        url = d["url"]
        if record_holder[url] == 0:
            record_holder[url] = [d]
            unique_links.append(url)
            continue
        record_holder[url].append(d)
    n = len(unique_links)

    if data:
        print('Starting Initial Site Scrape')
        def start_requests(self):
            yield from self.req_details(BullsEye.unique_links)

    def req_details(self, links:list):
        for l in links:
            if BullsEye.already_requested[l] == 0:
                yield ScrapflyScrapyRequest(
                    scrape_config=ScrapeConfig(url=l, rendering_wait=5000, retry=False, render_js=True, country='us'),
                    callback=self.parse, errback=self.handle_error, meta={'download_timeout': 15}
                )
                BullsEye.already_requested[l] =1
            else:
                yield from self.stored_parse(l)


    def _record_check(self, url, origin):
        """Assist debugging concerning the link to record mapping"""
        try:
            assert BullsEye.record_holder[url] != 0
        except:
            print(f'{origin}: Failure to access Record through request url{url}')
            data = {"fail_origin": origin, "req_url":url}
            currently_held = []
            for q in BullsEye.record_holder:
                print('Records: ' + q, BullsEye.record_holder[q][0]['image'], '\n')
                currently_held.append({"key":q,"value":BullsEye.record_holder[q]})
            data["currently_held"] = currently_held

    def handle_error(self, failure):
        """Handle critical Failures within generator obj"""
        status = re.compile(r'[0-9]{3,4}').findall(str(failure.value))
        if status:
            status = status[0]
        else:
            status = 404
        print(status)
        content = 'text/html'
        response_data = {"status": status, "html": '',"content":content,"request_url": failure.request.url}
        if BullsEye.initial_response_phase:
            return self.resp_landing(response_data)
        return self.img_landing(response_data)

    def parse(self, response):
        status = response.status
        html = response.text
        content = str(response.headers.get('content-type'))
        BullsEye.stored_html[response.request.url] = html
        BullsEye.stored_content[response.request.url] = content
        BullsEye.stored_status[response.request.url] = status
        response_data = {"status": status, "html": html, "content":content,"request_url": response.request.url}
        if BullsEye.initial_response_phase:
            return self.resp_landing(response_data)
        return self.img_landing(response_data)

    def stored_parse(self,url):
        """Get saved data on urls that have already been requested"""
        status = BullsEye.stored_status[url]
        content = BullsEye.stored_content[url]
        html = BullsEye.stored_html[url]
        response_data = {"status":status,"html": html, "content":content, "request_url":url}
        self._record_check(origin='Stored Parse', url=url)
        if BullsEye.initial_response_phase:
            return self.resp_landing(response_data)
        return self.img_landing(response_data)

        #return self.decipher_image(response_data)

    def resp_landing(self, response_data: dict):
        """Landing for the initial HTML response, parse and save image blocks"""
        BullsEye.req_count += 1
        url = response_data["request_url"]
        status = response_data["status"]
        html = response_data["html"]

        asset_list = BullsEye.record_holder[url]
        if asset_list != 0:
            for asset in asset_list:
                asset["response"] = status
                asset["request_url"] = url

                if not html:
                    BullsEye.bad_requests.append(asset)

                elif status != 403 and status != 200:
                    BullsEye.bad_requests.append(asset)

                elif status == 403:
                    BullsEye.blocked_or_empty.append(asset)

                else:
                    soup = BeautifulSoup(html, 'html.parser')
                    # asset['html'] = html
                    "Catch sites that return no images at all ( blocked Or PDF)"
                    if not soup.find_all('img') and not soup.find_all('meta'):
                        BullsEye.blocked_or_empty.append(asset)
                    else:
                        meta_block = f"{soup.find_all('meta')}"
                        image_block = f"{soup.find_all('img')}"

                        asset["meta_block"] = meta_block
                        asset["image_block"] = image_block

                        meta_soup = BeautifulSoup(meta_block, 'html.parser')
                        image_soup = BeautifulSoup(image_block, 'html.parser')
                        all_images = self.isolate_images(meta_soup, image_soup, url)

                        if all_images:
                            asset["image"] = all_images[0]
                            asset["possible_images"] = all_images

                            BullsEye.check_images.append(asset)
                        else:
                            BullsEye.blocked_or_empty.append(asset)

        print(f"Has: {len(BullsEye.check_images)} Hasnt: {len(BullsEye.blocked_or_empty + BullsEye.bad_requests)}\n")
        print(f"HTML Phase: Count: {BullsEye.req_count} --> N:{BullsEye.n}")

        if BullsEye.req_count == BullsEye.n:
            js = open(BullsEye.blocked_or_empty_path, "w")
            js.write(json.dumps({"Row": BullsEye.blocked_or_empty}, indent=2, sort_keys=True))
            js.close()

            js = open(BullsEye.check_images_path, "w")
            js.write(json.dumps({"Row": BullsEye.check_images}, indent=2, sort_keys=True))
            js.close()
            js = open(BullsEye.bad_request_path, "w")
            js.write(json.dumps({"Row": BullsEye.bad_requests}, indent=2, sort_keys=True))
            js.close()

            BullsEye.initial_response_phase = False
            BullsEye.req_count = 0
            BullsEye.record_holder = Counter()
            BullsEye.unique_links = []
            BullsEye.none_found = BullsEye.blocked_or_empty
            # start Requests to check image links
            print('Starting Image Check')
            for row in BullsEye.check_images:
                url = row["image"]
                image_link = self.memoize_record(url, row)
                if image_link:
                    BullsEye.unique_links.append(image_link)

            BullsEye.n = len(BullsEye.unique_links)
            yield from self.req_details(BullsEye.unique_links)

    def isolate_images(self, meta_block, image_block, main_url):
        """pick the best m images from the page"""
        already_seen = Counter()
        validate = validations()
        image_count = 0
        image_cap = 10

        meta_image = []
        logo_priority = []
        other_images = []

        for item in meta_block.find_all('meta'):
            link = item.get('property')
            if link and link == 'og:image':

                check_image = validate.image_file_type(validate.confirm_url(item.get('content'), main_url))
                if check_image:
                    image_count += 1
                    meta_image.append(check_image)
                    break

        for item in image_block.find_all('img'):
            src_link = item.get('src')
            if src_link:
                check_image = validate.image_file_type(validate.confirm_url(src_link, main_url))
                if check_image and already_seen[check_image] == 0:
                    logo_match = re.compile(rf'(?i)logo', flags=re.IGNORECASE).findall(check_image)
                    image_count += 1
                    if logo_match:
                        logo_priority.append(check_image)
                    else:
                        other_images.append(check_image)
                    already_seen[check_image] = 1
                if image_count == image_cap:
                    break
        all_images = meta_image + logo_priority + other_images
        return all_images

    def img_landing(self, response_data:dict):
        BullsEye.req_count += 1

        content =response_data["content"]
        status = response_data["status"]
        request_url = response_data["request_url"]
        asset_list = BullsEye.record_holder[request_url]
        if asset_list != 0:
            for asset in asset_list:
                asset['content_type'] = content
                asset['img_response'] = status

                if not asset['content_type']:
                    print(f"No content type:{request_url}  {content}")
                    BullsEye.none_found.append(asset)

                elif status == 404 or 'image' not in content:
                    print('Bad:',response_data["request_url"],status,'\n')

                    if len(asset["possible_images"]) >1:
                        # add the record to the queue, this time with a new image
                        asset["possible_images"].pop(0)
                        asset["image"] = asset["possible_images"][0]
                        BullsEye.queue.append(asset)
                    else:
                        BullsEye.none_found.append(asset)
                else:

                    BullsEye.found.append(asset)
                    #print('good:', asset["image"], asset['content_type'], asset['img_response'], '\n')

        print(f"IMG Phase: Request Count:{BullsEye.req_count}  N:{BullsEye.n}")


        if  BullsEye.req_count==BullsEye.n and BullsEye.queue:
            time.sleep(15)
            print(f'Found = {len(BullsEye.found)}')
            print(f'trying again with {len(BullsEye.queue)} records')
            # reset base case
            next_round = BullsEye.queue
            BullsEye.queue = []
            BullsEye.record_holder = Counter()
            BullsEye.req_count = 0
            BullsEye.unique_links = []
            for row in next_round:
                url = row["image"]
                image_link = self.memoize_record(url, row)
                if image_link:
                    BullsEye.unique_links.append(image_link)

            # make call with new dataset

            BullsEye.n = len(BullsEye.unique_links)
            yield from self.req_details(BullsEye.unique_links)

        elif BullsEye.queue == [] and BullsEye.req_count == BullsEye.n and BullsEye.used_backup is False and BullsEye.none_found !=[]:
                """Last try on the stubborn sites usiing Clearbit"""
                BullsEye.used_backup = True
                print('Trying clear_bit backup')

                validate = validations()
                clear_bit = 'https://logo.clearbit.com/'
                last_round = BullsEye.none_found
                BullsEye.none_found = []

                BullsEye.req_count = 0
                BullsEye.record_holder = Counter()

                BullsEye.unique_links = []
                for row in last_round:
                    main_url = row['url']
                    row["image"] = clear_bit+ validate.get_base_url(main_url)
                    url = row["image"]
                    row['possible_images'] = []
                    image_link = self.memoize_record(url,row)
                    print(image_link)
                    if image_link:
                        BullsEye.unique_links.append(image_link)
                BullsEye.n = len(BullsEye.unique_links)
                yield from self.req_details(BullsEye.unique_links)


        elif BullsEye.queue == [] and BullsEye.req_count==BullsEye.n:
            print("Finished")
            js = open(BullsEye.found_path, "w")
            js.write(json.dumps({"Row": BullsEye.found}, indent=2, sort_keys=True))
            js.close()

            js = open(BullsEye.none_found_path, "w")
            js.write(json.dumps({"Row": BullsEye.none_found}, indent=2, sort_keys=True))
            js.close()

    def memoize_record(self,url,record):
        """For every recursive call, remap the new links to the existing records"""
        image = None
        if BullsEye.record_holder[url] == 0:
            BullsEye.record_holder[url] = [record]
            image = url
            #print(BullsEye.unique_links)
        else:
           BullsEye.record_holder[url].append(record)
        return image



'''if __name__ == '__main__':
    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(BullsEye)
    start = perf_counter()

    process.start()
    stop = perf_counter()
    print("time taken:", stop - start)'''





