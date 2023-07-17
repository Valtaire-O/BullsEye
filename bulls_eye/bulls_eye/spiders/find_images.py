import time
import json
from scrapfly.scrapy import  ScrapflyScrapyRequest,ScrapflyCrawlSpider
from scrapfly import ScrapeConfig
from bulls_eye.db_connect import BaseQuery
from  bulls_eye.img_processing import Preprocessor, PostProcessor,TorchImage
from bs4 import BeautifulSoup
import re
from collections import Counter



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
    name = 'imagescrape'

    check_images, none_found = [], []
    found, unique_links, queue = [], [], []
    stop_images = []
    req_count, throttle_count = 0, 0

    parent_path = '/Users/valentineokundaye/PycharmProjects/BullsEye'

    record_holder, already_seen, already_requested,og_meta = Counter(), Counter(), Counter(),Counter()
    # hold stored response info
    stored_content_type, stored_status, stored_html, stored_response_content = Counter(), Counter(), Counter(), Counter()

    # initialize image processor classes
    link_validations, post_processor = Preprocessor(), PostProcessor()
    #torch_vision = TorchImage()

    make_query = BaseQuery()

    """These vars are used to toggle Bullseye between Verification mode and Find Image mode"""
    initial_response_phase = False
    verification_phase = False
    used_backup = False

    verify_img_data = make_query.verify_img_data()
    find_img_data = make_query.find_img_data()
    n = 0
    if verify_img_data:
        verification_phase = True
        # memoize by image, yeild request
        for d in verify_img_data:
            # memoize the initial data set to the key of unique links
            url = d["image"]
            # append records with the same url to a list value where the key is the unique url
            if record_holder[url] == 0:
                record_holder[url] = [d]
                unique_links.append(url)

                continue
            record_holder[url].append(d)

        n = len(unique_links)
        print('Verifying images...')

    else:
        initial_response_phase = True
        # memoize by image
        for d in find_img_data:
            # memoize the initial data set to the key of unique links
            url = d["main_url"]

            # append records with the same url to a list value where the key is the unique url
            if record_holder[url] == 0:
                record_holder[url] = [d]
                unique_links.append(url)

                continue
            record_holder[url].append(d)

        n = len(unique_links)
        print(f'Finding Images... for {n} records')

    if n >=1:
        def start_requests(self):
            yield from self.req_details(BullsEye.unique_links)

    def req_details(self, links:list):
        proxy_pool = 'public_residential_pool'
        for l in links:
            if BullsEye.initial_response_phase is True:
                yield ScrapflyScrapyRequest(
                    scrape_config=ScrapeConfig(url=l, rendering_wait=5000, retry=False, render_js=True, country='us',
                                               proxy_pool=proxy_pool),
                    callback=self.parse, errback=self.handle_error, dont_filter=True, meta={'download_timeout': 15}
                )
            else:
                # no need to render js when getting images links
                yield ScrapflyScrapyRequest(
                    scrape_config=ScrapeConfig(url=l, retry=False, country='us', proxy_pool=proxy_pool),
                    callback=self.parse, errback=self.handle_error, dont_filter=True,meta={'download_timeout': 10}
                )


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

        content_type = 'text/html'
        response_content = 'None'
        response_data = {"status": status, "html": '', "content_type":content_type,"request_url": failure.request.url,
                         "response_content":response_content}

        if BullsEye.initial_response_phase:
            return self.resp_landing(response_data)
        return self.img_landing(response_data)

    def parse(self, response):

        """All godd responses come here """
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

        #print(f"Has: {len(BullsEye.check_images)} Hasnt: {len(BullsEye.none_found)}\n")
        #print(f"HTML Phase: Count: {BullsEye.req_count} --> N:{BullsEye.n}")

        if BullsEye.req_count == BullsEye.n:
            """Indicates that the initial site scraping phase has finished"""


            BullsEye.initial_response_phase = False
            BullsEye.req_count = 0
            BullsEye.record_holder = Counter()
            BullsEye.unique_links = []
            # start Requests to check image links
            print('Starting Image processing...')
            for row in BullsEye.check_images:
                url = row["image"]
                image_link = self.memoize_record(url, row)
                if image_link:
                    BullsEye.unique_links.append(image_link)

            BullsEye.n = len(BullsEye.unique_links)
            yield from self.start_requests()

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
                    BullsEye.og_meta[check_image] = True
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

    def enqueue(self,asset:list,reason):
        """Expects a list of unique records"""
        """if an image fails the processing checks, add the next available image to the queue,
         if none is avaliable add to none found"""
        if len(asset["possible_images"]) > 1:
            # add the record to the queue, this time with a new image
            asset["possible_images"].pop(0)
            asset["image"] = asset["possible_images"][0]
            BullsEye.queue.append(asset)
            return 0
        asset['reason'] = reason
        #
        BullsEye.none_found.append(asset)

        return 0

    def img_landing(self, response_data:dict):
        BullsEye.req_count += 1
        print(f"IMG Phase: Request Count:{BullsEye.req_count}  N:{BullsEye.n}")
        BullsEye.throttle_count +=1
        if BullsEye.throttle_count ==2500:
            time.sleep(60)
            print('Sleeping..')
            BullsEye.throttle_count = 0
        blank = False
        validate = Preprocessor()
        #torch_vision = BullsEye.torch_vision

        post_processor = BullsEye.post_processor
        response_content = response_data["response_content"]
        content_type =response_data["content_type"]
        status = response_data["status"]
        request_url = response_data["request_url"]
        good_img = False
        has_white = False

        asset_list = BullsEye.record_holder[request_url]
        
        if asset_list != 0:
            
            img = post_processor.open_img(response_content=response_content)
            # print(img)
            if img and status == 200 and 'image' in content_type:
                # check for transparency

                if 'png' in content_type or 'webp' in content_type:
                    """For Formats check for transparency, then check for white colored pixels"""
                    is_transparent = post_processor.check_transparency(img)
                    if is_transparent:
                        has_white = post_processor.check_for_whiteness(img)
                        if has_white:
                            # todo: Currently hardcoding the background colors based on
                            #  whiteness, later will pass in color of main site header

                            img = post_processor.resolve_transparency(img, background_color='#000000')
                            good_img = False # if white and transparent try clearbit
                            for i in asset_list:
                                i['possible_images'] = []
                        else:
                            img = post_processor.resolve_transparency(img, background_color='#FFFFFF')

                """Check for blank images"""
                if has_white is False:
                    is_blank = post_processor.check_blanks(img)
                    if is_blank:
                        #'blank_image
                        good_img = False
                    elif BullsEye.og_meta[request_url] is True:
                        BullsEye.found.extend(asset_list)
                        good_img = True

                        '''elif BullsEye.used_backup is False:
                        """After we have checked for transparency and blankness,
                                             check for unwanted images"""
                        #only checking for known bad images for assorted not defaults
                        stop_image = torch_vision.compare_new_image(img)
                        if stop_image:
                            BullsEye.stop_images.extend(asset_list)
                            #print(f"Stop Image: {request_url}")
                            good_img = False
                        else:
                            # passed blank and stop image test
                            BullsEye.found.extend(asset_list)
                            good_img = True'''

                    else:

                        BullsEye.found.extend(asset_list)
                        good_img = True

            else:
                # unable to open image
                if BullsEye.og_meta[request_url] is True and 'svg' in content_type:
                    BullsEye.found.extend(asset_list)
                    good_img = True
                else:
                    good_img = False

            print(f"IMG Phase: Request Count:{BullsEye.req_count}  N:{BullsEye.n}")
            if BullsEye.verification_phase is True:
                return self.verify_images([good_img,asset_list])

            return self.find_new_images([good_img,asset_list])


    def verify_images(self,img_data):
        found = img_data[0]
        records = img_data[1]

        if found is False:
           BullsEye.none_found.extend(records)


        if BullsEye.req_count == BullsEye.n:
            find_img_data = BullsEye.make_query.find_img_data()
            if find_img_data:
                BullsEye.verification_phase = False
                BullsEye.initial_response_phase = True
                BullsEye.record_holder = Counter()
                BullsEye.req_count = 0
                BullsEye.unique_links = []

                for row in find_img_data:
                    url = row["main_url"]
                    image_link = self.memoize_record(url, row)
                    if image_link:
                        BullsEye.unique_links.append(image_link)

                # make call with new dataset

                BullsEye.n = len(BullsEye.unique_links)
                yield from self.req_details(BullsEye.unique_links)


    def find_new_images(self,img_data):
        found = img_data[0]
        records = img_data[1]

        if found is False:
            print(f"Sanity check : {len(set([i['record_name'] for i in records]))== len(records)}")
            for i in records:
               self.enqueue(i, 'bad_image')



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
                """Last try on the stubborn sites using backup image finder api('s)"""
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
                    main_url = row['main_url']
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

            print(f'Found = {len(BullsEye.found)}  None Found = {len(BullsEye.none_found)}')
            print(f'Unique_set = {len(set( [i["stage_id"] for i in BullsEye.found + BullsEye.none_found] )) }')
            print("Finished...Updateing tables")
            BullsEye.make_query.update_found(BullsEye.found, verification=False)
            BullsEye.make_query.update_none_found(BullsEye.none_found, verification=False)

    def memoize_record(self,url,record):
         """For every recursive call, remap the new links to the existing records"""
         image = None
         if BullsEye.record_holder[url] == 0:
            BullsEye.record_holder[url] = [record]
            image = url # return only the unique instance
            #print(BullsEye.unique_links)
         else:
             BullsEye.record_holder[url].append(record)
         return image






