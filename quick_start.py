from controller import ImageFinder
import requests
import json
#test with bad links
"""swith open('test_stuff/test_data/error_link_tests.json') as f:
         links = json.load(f)["Row"]"""

with open('test_stuff/test_data/mostly_good_links.json') as f:
    links = json.load(f)["Row"]

find_images = ImageFinder().start(links[:5])
