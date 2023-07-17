from dagster import MetadataValue, OpExecutionContext, asset, op

#from extracts.extracts.spiders.news import NewsSpider

from bulls_eye.bulls_eye.spiders.find_images import  BullsEye
from scrapy.crawler import CrawlerProcess
from bulls_eye.img_processing import  Preprocessor
from bulls_eye.db_connect import BaseQuery



gname = "Image_ETL"

settings ={
            'BOT_NAME': 'bulls_eye',
            'NEWSPIDER_MODULE': 'bulls_eye.spiders',
            'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
            'SPIDER_MODULES':  ['bulls_eye.bulls_eye.spiders'],
            'SCRAPFLY_API_KEY': 'e5e77e603ae744b0aa408e4f0b59cd61',
            #'LOG_LEVEL' : 'INFO',
            'LOG_ENABLED': False,
            'CONCURRENT_REQUESTS': 20
        }

make_query = BaseQuery(testing=True)

@asset(group_name=gname)
def check_database() :
    '''This task queries the database for records in need of image processing'''
    """Start by ingesting records from DB,
            expects that all records will have hyperlinks"""

    validate = Preprocessor()

    data = make_query.select_live_staging()
    if len(data) <1:
        return False
    print(f"selected {len(data)} records")
    for d in data:
        confirm_url = validate.confirm_url(d['mainURL'])
        if not confirm_url:
            continue
        d['main_url'] = confirm_url
    make_query.save_to_copy_db(data)
    return  True


@asset(group_name=gname)
def site_extraction(check_database):
    '''Branch the pipeline depending on the output report of the ingestion task'''
    #print("First We scrape...")
    if  check_database:
        process = CrawlerProcess(settings=settings)
        process.crawl(BullsEye)
        try:
            process.start()
            return True
        except:
            return False
    else:
        return False

'''@asset(group_name=gname)
def image_processing(event_validate_input):
    if event_validate_input:
        try:
            result = ProductionLoad().start_insert(asset_type)
            return result
        except:
            """Return false if any unexpected issue occur"""
            return {"valid":False}'''



@asset(group_name=gname)
def update_temp_db(site_extraction):
    '''This task sends the report of successful distribution '''
    if site_extraction:
        make_query.update_live_staging()
    return 'All Done'

'''@asset(group_name=gname)
def  update_live_db(event_distribution):
   
    if event_distribution:
        if event_distribution["valid"]:
            message = shared.success_message(asset=asset_type) +\
                      event_distribution["info"] +"BROUGHT TO YOU BY Dagster"
            shared.send_slack(message, True)
            return 0'''




'''
dagster dev -m etl_dags
'''