from importlib import import_module
from twisted.internet import reactor, defer
from bulls_eye.shared_classes import Preprocessor
from scrapy.crawler import CrawlerProcess
import json
from collections import Counter
from time import perf_counter
import sys
from scrapy.crawler import CrawlerRunner
import  time
import  os
import scrapy
import scrapy.crawler as crawler
from scrapy.utils.log import configure_logging
from multiprocessing import Process, Queue
from twisted.internet import reactor


class ImageFinder:

    def __init__(self):
        self.settings ={
            'BOT_NAME': 'bulls_eye',
            'NEWSPIDER_MODULE': 'bulls_eye.spiders',
            'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
            'SPIDER_MODULES':  ['bulls_eye.bulls_eye.spiders'],
            'SCRAPFLY_API_KEY': 'e5e77e603ae744b0aa408e4f0b59cd61',
            'LOG_LEVEL' : 'INFO',
            'CONCURRENT_REQUESTS': 20
        }
    def start(self, links:list):
        # empty out data sets
        js = open('bulls_eye/bulls_eye/spiders/site_data/check_images.json', "w")
        js.write(json.dumps({"Row": []}, indent=2, sort_keys=True))
        js.close()
        js = open('bulls_eye/bulls_eye/spiders/site_data/start_set.json', "w")
        js.write(json.dumps({"Row": []}, indent=2, sort_keys=True))
        js.close()

        validate = Preprocessor()
        keep,toss = [],[]
        count = 0
        already_seen = Counter()

        for l in links:
            if already_seen[l] !=0:
                continue

            already_seen[l] = 1
            count +=1
            confirm_url = validate.confirm_url(l)
            if confirm_url:
                keep.append({"url":confirm_url,"id":count})
                continue
            toss.append({"url":l,"id":count})

        js = open('bulls_eye/bulls_eye/spiders/site_data/rejected.json', "w")
        js.write(json.dumps({"Row": toss}, indent=2, sort_keys=True))
        js.close()

        js = open('bulls_eye/bulls_eye/spiders/site_data/start_set.json', "w")
        js.write(json.dumps({"Row": keep}, indent=2, sort_keys=True))
        js.close()

        return self._start_img_spider()

    '''def dci_start(self, data:list):
        # empty out data sets
        

        validate = Preprocessor()
        keep,toss = [],[]
        count = 0
        already_seen = Counter()

        for d in data:
            count +=1
            confirm_url = validate.confirm_url(d['mainURL'])
            if confirm_url:
                d['url'] = d['mainURL']
                del d['mainURL']
                keep.append(d)
                continue
            toss.append(d)

        js = open('bulls_eye/bulls_eye/spiders/image_data/none_found.json', "w")
        js.write(json.dumps({"Row": toss}, indent=2, sort_keys=True))
        js.close()

        js = open('bulls_eye/bulls_eye/spiders/site_data/start_set.json', "w")
        js.write(json.dumps({"Row": keep}, indent=2, sort_keys=True))
        js.close()

        self._temp_img_spider()'''


    def _start_img_spider(self):

        from bulls_eye.bulls_eye.spiders.get_response import BullsEye

        process = CrawlerProcess(settings=self.settings)
        process.crawl(BullsEye)
        start = perf_counter()
        process.start()
        stop = perf_counter()
        with open('bulls_eye/bulls_eye/spiders/image_data/img_response_phase.json') as f:
            found_images = json.load(f)
        for f in found_images["none_found"] + found_images["found"]:
            if "image_block" and "meta_block" in f.keys():
                del f["image_block"]
                del f["meta_block"]
                del f["possible_images"]
        return found_images

    def temp_img_spider(self):

        from bulls_eye.bulls_eye.spiders.test_spider import QuickTest


        process = CrawlerProcess(settings=self.settings)
        process.crawl(QuickTest)
        start = perf_counter()
        process.start()
        stop = perf_counter()
        print("time taken:", stop - start)






'''lwith open('bulls_eye/bulls_eye/spiders/site_data/double_check_patched.json') as f:
    data = json.load(f)["Row"]'''

#test = ImageFinder().temp_img_spider()






