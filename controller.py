from bulls_eye.img_processing import Preprocessor
from scrapy.crawler import CrawlerProcess
import json
from time import perf_counter
from multiprocessing import Process, Queue
from bulls_eye.db_connect import BaseQuery


class ImageFinder:
    """This class controls the flow of input, scraping , intermediary storage and updates to the db"""
    def __init__(self):
        self.settings ={
            'BOT_NAME': 'bulls_eye',
            'NEWSPIDER_MODULE': 'bulls_eye.spiders',
            'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
            'SPIDER_MODULES':  ['bulls_eye.bulls_eye.spiders'],
            'SCRAPFLY_API_KEY': 'e5e77e603ae744b0aa408e4f0b59cd61',
            #'LOG_LEVEL' : 'INFO',
            'LOG_ENABLED': False,
            'CONCURRENT_REQUESTS': 20
        }

    def start(self):

        """Start by ingesting records from DB,
        expects that all records will have hyperlinks"""
        make_query = BaseQuery()
        validate = Preprocessor()

        #pulling from the DCI

        data = make_query.select_live_staging()
        for d in data:
            confirm_url = validate.confirm_url(d['mainURL'])
            if not confirm_url:
                continue

            d['main_url'] = confirm_url
            '''if d['image'] != 'n/a':
                stop_img = validate.stop_images(d['image'])
                if stop_img  is True:
                    d['image'] = 'n/a'''

        make_query.save_to_copy_db(data)
        self._start_bulls_eye()
        make_query.update_live_staging()





    def start_test(self):

        """Start by ingesting records from DB,
        expects that all records will have hyperlinks"""
        make_query = BaseQuery(testing=True)
        validate = Preprocessor()
        data = make_query.select_live_staging()

        for d in data:
            # find and fix any potential url mishaps
            confirm_url = validate.confirm_url(d['mainURL'])
            if not confirm_url:
                continue

            d['main_url'] = confirm_url

        make_query.save_to_copy_db(data)
        self._start_bulls_eye()
        make_query.update_live_staging()
        return "All Done"

    def _start_bulls_eye(self):
        """Runs the image scraper/processor"""
        from bulls_eye.bulls_eye.spiders.find_images import BullsEye
        process = CrawlerProcess(settings=self.settings)
        process.crawl(BullsEye)
        start = perf_counter()
        process.start()
        stop = perf_counter()
        print(f"finished in {stop - start }seconds")


ImageFinder().start_test()
'''
gcloud functions deploy the-bully-function \
  --gen2 \
  --region=us-central1 \
  --runtime=python311 \
  --source=. \
  --entry-point=ole_ole \
  --trigger-http \
  --memory=8G  \
  --max-instances=1 \
  --timeout=3600

'''