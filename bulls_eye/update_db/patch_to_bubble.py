import re
import requests
import json
from collections import Counter
import time
from time import perf_counter



class PostIt:
    with open('temp/bubble_ready.json') as f:
        final = json.load(f)["Row"]

    # with open('processed/image_store.json') as f:
    stored = []
    already_downloaded = Counter()

    def get_images(self):
        for s in PostIt.stored:
            image = s['logo']
            PostIt.already_downloaded[image] = s["storage"]

    def post_to_bucket(self, image):
        #Post Image URL to GCS Bucket
        data = {"imageUrl": image}
        data = json.dumps(data)

        headers = {
            'Content-Type': 'application/json',
            'x-api-key': 'AIzaSyD39d6Chbi4CUK8tEb7GKGm2IzARVT_BSM'
        }
        url = f'https://add-file-to-bucket-2-bkem9ey5.ue.gateway.dev/addfilenew'
        response = requests.post(url, headers=headers, data=data).json()
        return response

    def start(self):
        count = 0
        for l in PostIt.final:
            image = l['image']
            count += 1
            if PostIt.already_downloaded[image] ==0:
                response = self.post_to_bucket(image)
                print(response)
                try:
                    storage = response['imageUrl']
                    l["logo"] = response['imageUrl']
                    PostIt.already_downloaded[image] =  l["logo"]

                except:

                    pass




        js = open(f"patched_data/bubble_ready.json", "w")
        js.write(json.dumps({"Row": PostIt.final}, indent=2, sort_keys=True))
        js.close()

    def patch_it_baby(self,data):
        count = 0
        patched_false,patched_true = [], []
        for d in data:
            if 'logo' in d.keys():
                logo = d["logo"]
                bubble = d["dc_id"]
                record = {"logo": logo}
                update = self.update_bubble(json.dumps(record), bubble)

                if update.status_code != 204:
                    patched_false.append(d)
                    print(update.status_code, d, '\n')
                    continue
                print(update.status_code, '\n')
                patched_true.append(d)
            else:
                print(d)
                patched_false.append(d)

        js = open(f"patched_data/patched_true.json", "w")
        js.write(json.dumps({"Row": patched_true}, indent=2, sort_keys=True))
        js.close()

        js = open(f"patched_data/patched_false.json", "w")
        js.write(json.dumps({"Row": patched_false}, indent=2, sort_keys=True))
        js.close()

    def update_bubble(self,asset_record, id):
        datatype = 'Unified%20Assets'
        data = asset_record

        url = f'https://data.ecomap.tech/api/1.1/obj/{datatype}/{id}'

        headers = {
            'Authorization': f'Bearer 1b850b046f18ae20ed7116b62c3ddeec',
            'Content-Type': 'application/json'
        }
        response = requests.patch(url, headers=headers, data=data)
        return response


class PatchToPlat:
    eco = dci_get('Ecosystems', 'DCI', '1b850b046f18ae20ed7116b62c3ddeec').fillna('none')
    eco_url = dict(zip(eco['_id'], eco['X_API Endpoint']))
    eco_key = dict(zip(eco['_id'], eco['X_API Key']))