import uuid
from time import perf_counter
from db_connect import BaseQuery
from bullseye import BullsEye
import json
import random
import os
os.environ["GOOGLE_CLOUD_PROJECT"] = 'sunlit-shelter-377115'



def store_record(unique_id, name, url):
    data = {'stage_id': unique_id, 'record_name': name, 'url': url, 'image': 'n/a', "origin": 'endpoint'}
    BaseQuery().save_to_copy_db(data=[data])


def retrieve_record(stage_id):
    return BaseQuery().single_select_record(stage_id)


def bully_function(url,name,current_logo=None):
    '''all records stored in CLoud SQL instance'''


    '''create unique id for record'''
    unique_id = uuid.uuid4()
    store_record(unique_id, name, url)

    bulls_eye = BullsEye(stage_id=unique_id,current_logo=current_logo)

    bulls_eye.main_site_extract()

    results = bulls_eye.find_logo()
    if len(results) == 4:
        pass
    if not results:
        pass

    if current_logo:
        if len(results) == 4:
            image_url, similarity, og_image_url, message = results
            return {"url": url, "logo": image_url, "old_logo": current_logo,"og_image_url": og_image_url, "similarity": similarity,
                    'message': message}
        else:
            print(f'error: results = {results}')
            message = {'success': 'no new logo found'}
            return {"url": url, "logo": "","og_image_url":"", "old_logo": current_logo, "similarity": 0,
                    'message': message}
    else:
        if len(results) ==2:
            image_url, og_image_url = results
            return {"url": url, "logo": image_url, "og_image_url": og_image_url }
        else:
            print(f'error: results = {results}')
            return {"url": url, "logo": ""}
data = []
# prepare test set with correct json body
with open('test_set.txt') as f:
    for l in f:
        values = l.split()
        values.reverse()
        url = values.pop(0)
        current_logo = values.pop(0)
        values.reverse()
        name = " ".join(values)
        record = {"name": name, "current_logo": current_logo,"url": url, "endpoint": current_logo}

        data.append(record)



#data = [{'current_logo': 'https://storage.googleapis.com/eco_one_images/1683991348485.jpeg', 'name': 'The Avenue News', 'url': 'https://www.avenuenews.com/'}]
avg_time = []


random.shuffle(data)
found_count = 0

#data = [{'name': 'Letterspace Creative', 'current_logo': 'https://storage.googleapis.com/eco_one_images/1683989098789.jpeg', 'url': 'http://www.letterspacecreative.com/about.html'}]
for d in data[:20]:
    name = d["name"]
    current_logo = d["current_logo"]
    url = d["url"]
    start = perf_counter()
    bully_search = bully_function(url=url, name=name)
    stop = perf_counter()

    if bully_search['logo']:
        found_count += 1

    time = stop - start
    avg_time.append(time)
    print(bully_search)


print(f"Avg time taken: {sum(avg_time) / len(avg_time)}"
      f"Hit rate: {found_count/ len(data)}")




