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
from  bulls_eye.shared_classes import Preprocessor, PostProcessor,TorchImage
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



class BullsEye(ScrapflyCrawlSpider):
    """This class is designed to access any  webpage and return the image most
    likley to be relevant to the content on the page
    It has two phases the in first we hit the website and get the best m image links
    Then we pass the images through a series of checks to confirm its relevancy"""

    #handle_httpstatus_list = [i for i in range(300, 600)]
    name = 'imagescraper'

    site_folder = 'site_data'
    image_folder = 'image_data'
    spider_path = 'bulls_eye/bulls_eye/spiders'



    site_abs_path = os.path.abspath(f"{spider_path}/{site_folder}")
    image_abs_path = os.path.abspath(f"{spider_path}/{image_folder}")
    # Todo Improve this path toggle
    #Save Initial Html Response
    site_response_phase_path = f"{site_folder}/site_response_phase.json"
    img_response_phase_path = f"{image_folder}/img_response_phase.json"

    start_set_path = f"{site_folder}/start_set.json"

    initial_response_phase = True
    used_backup = False

    if __name__ != '__main__':
        site_response_phase_path = f"{site_abs_path}/site_response_phase.json"
        img_response_phase_path =  f"{image_abs_path}/img_response_phase.json"
        start_set_path = f"{site_abs_path}/start_set.json"


    with open(start_set_path) as f:
        data = json.load(f)["Row"]
    '''lwith open('bulls_eye/bulls_eye/spiders/site_data/in_dci.json') as f:
        data = json.load(f)["Row"][:50]'''



    check_images, none_found = [], []
    found, unique_links,queue = [], [], []
    req_count,throttle_count = 0,0


    parent_path = '/Users/valentineokundaye/PycharmProjects/BullsEye'

    record_holder, already_seen, already_requested = Counter(), Counter(),Counter()
    # hold stored response info
    stored_content_type, stored_status, stored_html, stored_response_content = Counter(), Counter(), Counter(),Counter()

    #initialize image processor classes
    link_validations, post_processor = Preprocessor(), PostProcessor()
    torch_vision = TorchImage()

    for d in data:
        # memoize the initial data set to the key of unique links
        url = d["url"]

        if record_holder[url] == 0:
            record_holder[url] = [d]
            unique_links.append(url)
            continue
        record_holder[url].append(d)
    n = len(unique_links)
    print(n)

    if data:
        print('Starting Initial Site Scrape')
        def start_requests(self):
            yield from self.req_details(BullsEye.unique_links)

    def req_details(self, links:list):
        for l in links:
            if BullsEye.already_requested[l] == 0:
                if BullsEye.initial_response_phase is True:
                    yield ScrapflyScrapyRequest(
                        scrape_config=ScrapeConfig(url=l, rendering_wait=5000, retry=False, render_js=True, country='us'),
                        callback=self.parse, errback=self.handle_error, meta={'download_timeout': 15}
                    )
                else:
                    yield ScrapflyScrapyRequest(
                        scrape_config=ScrapeConfig(url=l, retry=False, country='us'),
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
            if status == '200': # prints "ignoring non 200 response"
                status = 404
        #print(status)
        content_type = 'text/html'
        response_content = 'None'
        response_data = {"status": status, "html": '', "content_type":content_type,"request_url": failure.request.url,
                         "response_content":response_content}

        if BullsEye.initial_response_phase:
            return self.resp_landing(response_data)
        return self.img_landing(response_data)

    def parse(self, response):
        status = response.status
        html = response.text
        response_content = response.content

        content_type = str(response.headers.get('content-type'))
        if not content_type:
            content_type = 'None'
        # store the response info by the request url
        BullsEye.stored_html[response.request.url] = html
        BullsEye.stored_content_type[response.request.url] = content_type
        BullsEye.stored_status[response.request.url] = status
        response_data = {"status": status, "html": html, "content_type":content_type,"request_url": response.request.url,
                         "response_content":response_content}

        if BullsEye.initial_response_phase:
            return self.resp_landing(response_data)
        return self.img_landing(response_data)

    def stored_parse(self,url):
        """Get saved data on urls that have already been requested"""
        response_content = BullsEye.stored_response_content[url]
        status = BullsEye.stored_status[url]
        content_type = BullsEye.stored_content_type[url]
        html = BullsEye.stored_html[url]
        response_data = {"status":status,"html": html, "content_type":content_type, "request_url":url,
                         "response_content":response_content}

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
                asset["site_response"] = status
                asset["request_url"] = url

                if not html:
                    asset["reason"] = 'bad_request'
                    BullsEye.none_found.append(asset)

                elif status != 403 and status != 200:
                    asset["reason"] = 'bad_request'
                    BullsEye.none_found.append(asset)

                elif status == 403:
                    asset["reason"] = 'blocked_or_empty'
                    BullsEye.none_found.append(asset)

                else:
                    soup = BeautifulSoup(html, 'html.parser')
                    # asset['html'] = html
                    "Catch sites that return no images at all"
                    if not soup.find_all('img') and not soup.find_all('meta'):
                        BullsEye.none_found.append(asset)
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
                            BullsEye.none_found.append(asset)

        print(f"Has: {len(BullsEye.check_images)} Hasnt: {len(BullsEye.none_found)}\n")
        print(f"HTML Phase: Count: {BullsEye.req_count} --> N:{BullsEye.n}")

        if BullsEye.req_count == BullsEye.n:
            js = open(BullsEye.site_response_phase_path, "w")
            js.write(json.dumps({"check_images": BullsEye.check_images,"none_found":BullsEye.none_found}, indent=2, sort_keys=True))
            js.close()

            BullsEye.initial_response_phase = False
            BullsEye.req_count = 0
            BullsEye.record_holder = Counter()
            BullsEye.unique_links = []
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
        validate = Preprocessor()
        image_count = 0
        image_cap = 10

        meta_image = []
        logo_priority = []
        other_images = []

        for item in meta_block.find_all('meta'):
            link = item.get('property')
            if link and link == 'og:image':

                check_image = validate.pre_processor(validate.confirm_url(item.get('content'), main_url))
                if check_image:
                    image_count += 1
                    meta_image.append(check_image)
                    break

        for item in image_block.find_all('img'):
            src_link = item.get('src')
            if src_link:
                check_image = validate.pre_processor(validate.confirm_url(src_link, main_url))
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
        # append images in order of priority
        all_images = meta_image + logo_priority + other_images
        return all_images

    def enqueue(self,asset,reason):
        """if an image fails the processing checks, add the next available image to the queue"""
        if len(asset["possible_images"]) > 1:
            # add the record to the queue, this time with a new image
            asset["possible_images"].pop(0)
            asset["image"] = asset["possible_images"][0]
            BullsEye.queue.append(asset)
            return 0
        asset['reason'] = reason
        BullsEye.none_found.append(asset)
        return False

    def img_landing(self, response_data:dict):
        BullsEye.req_count += 1
        BullsEye.throttle_count +=1
        if BullsEye.throttle_count ==2500:
            time.sleep(60)
            print('Sleeping..')
            BullsEye.throttle_count = 0
        blank = False
        validate = Preprocessor()
        torch_vision = BullsEye.torch_vision

        post_processor = BullsEye.post_processor
        response_content = response_data["response_content"]
        content_type =response_data["content_type"]
        status = response_data["status"]
        request_url = response_data["request_url"]

        asset_list = BullsEye.record_holder[request_url]
        is_valid = validate.pre_processor(request_url)
        if is_valid is None:
            [BullsEye.none_found.append(i) for i in asset_list if asset_list != 0]
            asset_list = 0
        if asset_list != 0:
            for asset in asset_list:
                asset['content_type'] = content_type
                asset['img_response'] = status
                img = post_processor.open_img(response_content=response_content)
                #print(img)
                if img and status==200 and 'image' in content_type:
                    # check for transparency

                    if 'png' in content_type or 'webp' in content_type:
                        """For Formats check for transparency, then check for white colored pixels"""
                        is_transparent = post_processor.check_transparency(img)
                        if is_transparent:
                            has_white = post_processor.check_for_whiteness(img)
                            if has_white:
                                # todo: Currently hardcoding the background colors based on
                                #  whiteness, later will pass in color of main site header


                                 img = post_processor.resolve_transparency(img,background_color='#000000')

                            else:

                                 img = post_processor.resolve_transparency(img, background_color='#FFFFFF')


                        else:
                            is_blank = post_processor.check_blanks(img)
                            if is_blank:
                                blank = True
                                print(f"Blank Image: {request_url}")
                                self.enqueue(asset,reason='blank_image')
                            '''else:
                                print(f"Found Image: {request_url}")
                                BullsEye.found.append(asset)'''

                    else:
                        """For non transparent formats, check for blanks"""
                        # Todo remove repetition
                        blank = True
                        is_blank = post_processor.check_blanks(img)
                        if is_blank:
                            print(f"Blank Image: {request_url}")
                            self.enqueue(asset,reason='blank_image')
                        '''else:
                            print(f"Found Image: {request_url}")
                            BullsEye.found.append(asset)'''

                    """After we have checked for transparency and blankness,
                     check for unwanted images"""
                    if blank is False and BullsEye.used_backup is False:
                        stop_image = torch_vision.compare_new_image(img)
                        if stop_image:
                            self.enqueue(asset, reason='stop_image')
                        else:
                            print(f"Found Image: {request_url}")
                            BullsEye.found.append(asset)
                else:
                    # unable to open image
                    self.enqueue(asset, reason='bad_image')


        print(f"IMG Phase: Request Count:{BullsEye.req_count}  N:{BullsEye.n}")


        if  BullsEye.req_count==BullsEye.n and BullsEye.queue:
            time.sleep(10)
            print(f'Found = {len(BullsEye.found)}')
            print(f'trying again with {len(BullsEye.queue)} records')
            # reset base case
            next_round = BullsEye.queue
            BullsEye.queue = [] # make sure to reset queue
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

                validate = Preprocessor()
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
                    #print(image_link)
                    if image_link:
                        BullsEye.unique_links.append(image_link)
                BullsEye.n = len(BullsEye.unique_links)
                yield from self.req_details(BullsEye.unique_links)


        elif BullsEye.queue == [] and BullsEye.req_count==BullsEye.n:
            print("Finished")
            js = open(BullsEye.img_response_phase_path, "w")
            js.write(json.dumps({"found": BullsEye.found, "none_found":BullsEye.none_found}, indent=2, sort_keys=True))
            js.close()

        '''js = open(BullsEye.img_response_phase_path, "w")
        js.write(json.dumps({"found": BullsEye.found, "none_found": BullsEye.none_found}, indent=2, sort_keys=True))
        js.close()'''




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





