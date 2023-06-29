from controller import ImageFinder
import requests
import json
from time import perf_counter
#test with bad links
"""swith open('test_stuff/test_data/error_link_tests.json') as f:
         links = json.load(f)["Row"]"""

with open('test_stuff/test_data/mostly_good_links.json') as f:
    links = json.load(f)["Row"][:200]


start = perf_counter()
find_images = ImageFinder().start(links)
stop = perf_counter()
print("time taken:", stop - start)

print(json.dumps(find_images,indent=2))

#

'''

virtualenv --python=python3.11 "/Users/valentineokundaye/PycharmProjects/BullsEye/env"

python3.11 -m venv venv
source activate/bin/venv
'''