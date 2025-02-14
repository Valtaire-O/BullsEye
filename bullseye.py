import json
from bs4 import BeautifulSoup
import re
import ast
from cv_models import CVModels, ClipModel
import io
from time import perf_counter
from google.cloud import storage
from img_processing import Preprocessor, StartProcess, PostProcessor
from async_api import Asnyc_API
from db_connect import BaseQuery
from typing import Optional, List, Dict
import logging

logging.basicConfig(level=logging.INFO)
# logging.getLogger().disabled = False
from image_similarity import ComputeSimilarity
from collections import defaultdict


class BullsEye:
    ''' Find the correct logo for every input  website.
        Start by scraping & saving the html of the sites,
        selecting the best images, then running those though an
        image processing -> CV pipeline '''

    def __init__(self, stage_id=None, current_logo=None):
        self.stage_id = stage_id
        self.current_logo = current_logo
        self.img_processing = StartProcess()
        self.link_processor = Preprocessor()
        self.async_calls = Asnyc_API()
        self.db_connector = BaseQuery()
        self.queue = []
        self.found = []
        self.none_found = []
        self.candidate_set = defaultdict(self.default_value)
        self.model_inference = CVModels()
        self.model_endpoint = self.model_inference.get_endpoint()
        self.model_headers = self.model_inference.get_headers()
        self.multimodal_inference = ClipModel()
        self.multimodal_endpoint = self.multimodal_inference.get_endpoint()
        self.logo_similarity = ComputeSimilarity()

    def main_site_extract(self):
        '''For each website , select a set of possible images
        Selects data from db, makes async request to each site
        gets back list of records containing the extracted html,
        from this call the selection mech. to collect the best images'''
        if self.stage_id:
            data = self.db_connector.site_extract_data(stage_id=self.stage_id)
        else:
            # process a large batch run
            data = self.db_connector.site_extract_data()

        extracted_data = self.async_calls.make_calls(data, headers={}, scrape_state=True)

        none_found, found_images = [], []

        for d in extracted_data:
            html = d['html']
            status = d["status"]
            url = d["main_url"]
            base_url = d["base_url"]

            if not html:
                none_found.append(d)

            elif status != 200:
                none_found.append(d)

            else:
                soup = BeautifulSoup(html, 'html.parser')

                # Catch sites that return no images at all
                if not soup.find_all('img') and not soup.find_all('meta'):
                    none_found.append(d)
                else:
                    meta_block = f"{soup.find_all('meta')}"
                    image_block = f"{soup.find_all('img')}"

                    d["meta_block"] = meta_block
                    d["image_block"] = image_block

                    meta_soup = BeautifulSoup(meta_block, 'html.parser')
                    image_soup = BeautifulSoup(image_block, 'html.parser')
                    get_images = self._candidate_selection(meta_soup, image_soup, base_url)
                    meta_image = get_images[0]
                    all_images = get_images[1]

                    if all_images:
                        d["meta_image"] = meta_image
                        d["possible_images"] = all_images

                        found_images.append(d)
                    else:
                        none_found.append(d)

        # add the image list to the record in the db
        self.db_connector.add_site_data(found_images, found=True)
        self.db_connector.add_site_data(none_found, found=False)

        logging.info(f"Candidate Selection\nFound: {len(found_images)} Not Found:{len(none_found)}")

    def _candidate_selection(self, meta_block, image_block, base_url):

        """
        Out of the N images present on the site, select a sparse candidate set of image
        links Based on the link text and surrounding html text ( alt & div text etc)
        """

        validate = self.link_processor
        image_count = 0
        top_k = 15  # cap on candidates
        already_seen = defaultdict(self.default_value)
        og_meta = defaultdict(self.default_value)

        meta_image = ''
        logo_priority = []
        other_images = []

        for item in meta_block.find_all('meta'):
            link = item.get('property')
            if link and link == 'og:image':
                check_image = validate.pre_processor(validate.confirm_url(item.get('content'), base_url))
                if check_image:
                    image_count += 1
                    meta_image = check_image
                    og_meta[check_image] = True
                    break

        for item in image_block.find_all('img'):
            src_link = item.get('src')
            if src_link:
                check_image = validate.pre_processor(validate.confirm_url(src_link, base_url))

                if check_image and already_seen[check_image] == 0:
                    logo_match = re.compile(rf'(?i)logo', flags=re.IGNORECASE).findall(check_image)
                    image_count += 1
                    if logo_match:
                        logo_priority.append(check_image)
                    else:
                        other_images.append(check_image)
                    already_seen[check_image] = 1
                if image_count == top_k:
                    break
        # append images in order of priority

        if meta_image != '':
            all_images = [meta_image] + logo_priority + other_images
        else:
            all_images = logo_priority + other_images

        return meta_image, all_images

    def find_logo(self):
        """From the selection set, process the images and choose the output image for that record."""
        if self.stage_id:
            # online endpoint runs
            data: List[Dict] = self.db_connector.image_discovery_data_single(stage_id=self.stage_id)
        else:
            # batch runs
            data: List[Dict] = self.db_connector.image_discovery_data()

        # run candidate logos through processing layers
        processed_candidates, batch_data = self._image_processing_layer(data)

        classify_candidates = self._classification_layer(processed_candidates, batch_data)
        if isinstance(classify_candidates, dict):
            return 0, {'error': 'Failure during classification stage, please retry'}

        output = self._output_layer(classify_candidates)

        logging.info(f'Found: {len(self.found)} Not Found: {len(self.none_found)}')

        self.db_connector.update_found(self.found, verification=False)
        self.db_connector.update_none_found(self.none_found, verification=False)
        # check similarity of new logo and previous logo
        if self.current_logo:
            # return output with message
            if output.found is False:
                return '', 0, '', {'success': 'no new logo to compare'}

            similarity = self._compare_logos(output.img_obj)
            if isinstance(similarity, dict):
                # a non int type similarity is an error message
                message = similarity
                return output.image_url, 0, output.og_image_url, message

            return output.image_url, similarity, output.og_image_url, {'success': 'comparison made'}
        else:
            return output.image_url, output.og_image_url

    def _image_processing_layer(self, data: List[Dict]):
        '''Run candidates through image processing'''
        # make requests to image links

        requested_images: List[Dict] = self.async_calls.make_calls(data, headers={}, image_state=True)

        candidates = []
        batch_data = []

        for record in requested_images:
            if record['status'] != 200:
                continue
            try:
                resp = ast.literal_eval(record['response'])
                img_content = io.BytesIO(resp)
            except:
                continue

            name = record["record_name"]

            response_data = {"content": img_content, "content_type": record["content_type"],
                             "request_url": record["endpoint"], "name": name}

            process_image = self.img_processing.process_image(response_data)

            if process_image['passed'] == False:
                continue

            # for candidates that pass through, prepare them for the next layer

            record['image_url'] = process_image['image_url']
            record['img_obj'] = process_image['img_obj']
            record['content'] = self.model_inference.prepare_img_content(input=record['img_obj'])
            record["og_image_url"] =  record["endpoint"]
            batch_data.append({"image_url": record['image_url'],"og_image_url": record["og_image_url"], "content": record["content"]})
            candidates.append(record)

        logging.info(f"{len(candidates)} Candidates made it through the image_processing layer")
        return candidates, batch_data

    def _classification_layer(self, data: List[Dict], batch_data: List[Dict]):
        '''Binary classification layer classes:logo or not_logo, filter out matches of the former'''
        if not data:
            return []

        record_map = defaultdict(self.default_value)
        # prepare data for batch classification
        preped_data = [{'body': self.model_inference.get_body(batch_data, task='batch_classify'),
                        'model_endpoint': self.model_inference.get_endpoint()}]
        candidates = []
        inference = []
        requested_images: List[Dict] = self.async_calls.make_calls(preped_data, headers=self.model_headers,
                                                                   api_state=True)

        for r in requested_images:

            if not r['response']:
                return {'error': 'classification layer failed to return a response'}
            # contains list of records processed via batch classification
            inference = r['response']

        for i in inference:
            image_url = i['image_url']
            model_response = i['response']
            record_map[image_url] = model_response

        for r in data:

            # prep for next task
            image_url = r['image_url']
            record_name = r['record_name']
            base_url = r['base_domain']

            prediction = record_map[image_url]['prediction']
            # reject images classed as non-logos
            if prediction == 'not_logo':
                continue

            labels = self.multimodal_inference.get_labels(record_name=record_name, base_domain=base_url)
            r['labels'] = labels
            r['body'] = self.multimodal_inference.get_body(input=image_url, labels=labels)
            r['model_endpoint'] = self.multimodal_endpoint
            candidates.append(r)

        logging.info(f"{len(candidates)} Candidates made it through the classify layer")
        return candidates

    def _output_layer(self, data: List[Dict]):
        '''Final output layer: make multimodal inference to predict the correct logo for the given site'''
        if not data:
            self.none_found.append({"stage_id": self.stage_id})
            return OutputObj()

        requested_images: List[Dict] = self.async_calls.make_calls(data, headers=self.model_headers, api_state=True)

        for r in requested_images:
            record_id = r['stage_id']
            img_obj = r['img_obj']
            image_url = r['image_url']
            og_image_url = r['og_image_url']

            if not r['response']:
                '''Todo: Need to add clip to dedicated infrence endpoint,
                until then add the 503 responses to the PQ'''
                candidate_image = CandidateObj(image_url, class_match=2, class_one=30,
                                               class_two=20, total_value=50,
                                               diff_threshold=10, img_obj=img_obj)
                swim_c = self._candidate_pq(record_id, candidate_image)
                continue
            label_1, label_2 = r['labels'][:2]
            verdict = self.multimodal_inference.visual_reasoning(image_url, label_1,
                                                                 label_2, r['response'])
            if verdict.reject:

                continue

            elif verdict.candidate:

                class_match = verdict.class_match
                class_one = verdict.class_one
                class_two = verdict.class_two
                total_value = class_one + class_two
                df = verdict.diff_threshold

                # create new candidate object and add to candidate queue
                candidate_image = CandidateObj(image_url,og_image_url=og_image_url, class_match=class_match, class_one=class_one,
                                               class_two=class_two, total_value=total_value,
                                               diff_threshold=df, img_obj=img_obj)
                swim_c = self._candidate_pq(record_id, candidate_image)

            elif verdict.output:
                # End process for record once output is found
                r['output_image'] = image_url
                self.found.append(r)

                return OutputObj(image_url=image_url,og_image_url=og_image_url, img_obj=img_obj, found=True)

        # If the logo has not been found and theres a candidate in the PQ
        if self.candidate_set[self.stage_id] != 0:
            # 0 subscript gets the first record in the queue
            image_url = self.candidate_set[self.stage_id][0].image_url
            img_obj = self.candidate_set[self.stage_id][0].img_obj
            self.found.append({"stage_id": self.stage_id, "output_image": image_url})
            return OutputObj(image_url=image_url,og_image_url=og_image_url, img_obj=img_obj, found=True)

        self.none_found.append({"stage_id": self.stage_id})
        return OutputObj()

    def _compare_logos(self, new_logo):
        '''Before comp, get the image content of the current logo'''

        # If possible, pull down bytes directly from bucket
        if 'storage.googleapis.com/eco_one_images/' in self.current_logo:
            '''pull bytes down from bucket'''
            image_id = self.current_logo.replace('https://storage.googleapis.com/eco_one_images/', '')
            current_logo = self._download_bucket_image(image_id=image_id, url=self.current_logo)
        else:
            '''make a quick request to the link'''
            current_logo = self._quick_scrape(url=self.current_logo)

        if current_logo:
            # Detect blanks, resolve potential transparency
            data = {"content": current_logo, "content_type": 'image/png',
                    "request_url": self.current_logo}

            process_image = self.img_processing.process_image(data)
            if process_image['passed'] == False:
                '''Logos are different'''
                return 0

            current_logo = process_image['img_obj']
            compute_similarity = self.logo_similarity.compare_logos(current_logo=current_logo,
                                                                    new_logo=new_logo)
            return compute_similarity

        return {'error': 'failed to download current_logo'}

    def _download_bucket_image(self, image_id, url):
        '''Pull down entire bucket folder, transform to raw bytes'''
        client = storage.Client('sunlit-shelter-377115')
        bucket = client.bucket('eco_one_images')
        try:
            blob = bucket.blob(image_id)
            content = io.BytesIO(blob.download_as_bytes())

            return content
        except:
            content = self._quick_scrape(url)
            return content

    def _quick_scrape(self, url):
        from scrapfly import ScrapflyClient, ScrapeConfig
        try:
            key = self.async_calls.api_key().replace('&key=', '')

            scrapfly = ScrapflyClient(key=key)
            response = scrapfly.scrape(
                ScrapeConfig(
                    url=url,
                    # we can set proxy country to appear as if we're connecting from US
                    country="US",
                    # for harder to scrape targets we can enable :anti-scraping protection bypass" if needed:
                    # asp=True,
                )
            )

            return response.content
        except:
            return None

    def _candidate_pq(self, record_id, candidate):
        """   Sort Images below the confidence threshold such that the
              first entry is the most likely logo.
              Swim up new candidate images based on sorting logic.
              The comparisons are based on the class of the image
              and it's total value, which is the sum of all positive scores.
              Each class has a threshold which is how much greater its total value
              must be to swim up
          """

        if self.candidate_set[record_id] == 0:
            self.candidate_set[record_id] = [candidate]
            return self.candidate_set[record_id]

        self.candidate_set[record_id].append(candidate)

        # the right position (n) is the newly appended candidate, we compare it to the image on its left
        n = len(self.candidate_set[record_id]) - 1
        c_value = candidate.total_value
        right_pos = n
        left_pos = n - 1

        while left_pos >= 0:
            # Every break condition stops the new candidate from swimming up the PQ,
            # if all are false, up we go
            if self.candidate_set[record_id][left_pos].class_match == self.candidate_set[record_id][
                right_pos].class_match:
                # If the classes are equal, swim up only if the new candidate's total value is greater
                if c_value <= self.candidate_set[record_id][left_pos].total_value:
                    break


            elif self.candidate_set[record_id][right_pos].class_match < self.candidate_set[record_id][
                left_pos].class_match:
                # If the candidates class is smaller than the current left node, swim up unless
                # the left nodes total value is x points higher than the new candidate's

                diff_threshold = self.candidate_set[record_id][left_pos].diff_threshold
                actual_diff = self.candidate_set[record_id][left_pos].total_value - c_value

                if actual_diff >= diff_threshold:
                    break

            elif self.candidate_set[record_id][left_pos].class_match < self.candidate_set[record_id][
                right_pos].class_match:
                # if the candidates class is greater than the current left node, only swim up if
                # the new candidates total value  is x points higher than the left node
                diff_threshold = candidate.diff_threshold
                actual_diff = c_value - self.candidate_set[record_id][left_pos].total_value

                if actual_diff < diff_threshold:
                    break

            # If we reach this point, the  candidate node swims up the PQ
            logging.info(
                f'swimming up candidate: class {candidate.class_match} total:{candidate.total_value} over left pos: class: {self.candidate_set[record_id][left_pos].class_match}'
                f'total: {self.candidate_set[record_id][left_pos].total_value}\n')
            swim_down = self.candidate_set[record_id][left_pos]
            swim_up = self.candidate_set[record_id][right_pos]
            self.candidate_set[record_id][left_pos] = swim_up
            self.candidate_set[record_id][right_pos] = swim_down
            right_pos -= 1
            left_pos -= 1

        return self.candidate_set[record_id]

    def default_value(self):
        return 0


class CandidateObj:
    """Candidate object for PQ"""

    def __init__(self, image_url,og_image_url, class_match, class_one, class_two, total_value, diff_threshold, img_obj):
        self.image_url = image_url
        self.og_image_url = og_image_url
        self.img_obj = img_obj
        self.class_match = class_match
        self.class_one = class_one
        self.class_two = class_two
        self.total_value = total_value
        self.diff_threshold = diff_threshold


class OutputObj:
    def __init__(self, image_url=None,og_image_url=None, found=False, img_obj=None):
        self.image_url = image_url
        self.og_image_url = og_image_url
        self.found = found
        self.img_obj = img_obj


