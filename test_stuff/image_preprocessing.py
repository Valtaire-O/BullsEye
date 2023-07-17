import json
import re
from collections import Counter
from bulls_eye.img_processing import Preprocessor
from bs4 import BeautifulSoup


with open('test_data/html_blocks.json') as f:
    data = json.load(f)["Row"]



def isolate_images(meta_block, image_block, main_url):
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

            check_image = validate.image_format(validate.confirm_url(item.get('content'), main_url))
            if check_image:
                image_count+=1
                meta_image.append(check_image)
                break

    for item in image_block.find_all('img'):
        src_link = item.get('src')
        if src_link:
            check_image = validate.image_format(validate.confirm_url(src_link, main_url))
            if check_image and already_seen[check_image] == 0:
                logo_match = re.compile(rf'(?i)logo', flags=re.IGNORECASE).findall(check_image)
                image_count += 1
                if logo_match:
                    logo_priority.append(check_image)
                else:
                    other_images.append(check_image)
                already_seen[check_image] = 1
            if image_count== image_cap:
                break
    all_images = meta_image + logo_priority + other_images

    return all_images
check_images,none_found = [],[]
all_images = []
[all_images.extend(i['possible_images']) for i in data]

'''for a in all_images:
    if 'instagram' in a :
        print(a)'''

for asset in data:
    
    url = asset['url']
    meta_block = asset["meta_block"]
    image_block = asset["image_block"]

    meta_soup = BeautifulSoup(meta_block, 'html.parser')
    image_soup = BeautifulSoup(image_block, 'html.parser')
    all_images = isolate_images(meta_soup, image_soup, url)

    if all_images:
        asset["image"] = all_images[0]
        asset["possible_images"] = all_images

        check_images.append(asset)
    else:
        none_found.append(asset)

test = re.compile(r'(?i)logo').findall('Logo')
