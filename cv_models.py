from db_connect import CloudConnect
from collections import defaultdict
import logging
import io


class CVModels:
    '''handles model inference functionality &information'''
    def __init__(self):
        self.auth_token = CloudConnect().get_model_api_key()

    def get_endpoint(self):
        return "https://zbj81iujfhi8qimk.us-east-1.aws.endpoints.huggingface.cloud"

    def get_headers(self):
        return {"Authorization": f"Bearer {self.auth_token}"}

    def get_task_name(self,classify=False,extract=False):
        arg_list = [classify,extract]
        if len([i for i in arg_list if i]) !=1:
            raise AttributeError('Pick one parameter to be true')

        if classify:
            return 'classify_batch'
        if extract:
            return 'extract'

    def get_body(self,input,task):
        #prep input for model
        if isinstance(input,list) :

            return {"inputs": {"input": input, "task": task},
                    "use_cache": True}


        if isinstance(input,bytes) :

            return {"inputs": {"input": f"{input}", "task": task},
                    "use_cache": True}


        img_byte_arr = self.prepare_img_content(input)
        return {"inputs": {"input": img_byte_arr, "task": task},
                "use_cache":True}

    def prepare_img_content(self,input):
        img_byte_arr = io.BytesIO()
        input.save(img_byte_arr, format='PNG')
        img_byte_arr = f"{img_byte_arr.getvalue()}"
        return img_byte_arr


class ClipModel(CVModels):
    def __init__(self):
        self.diff_map = {1:0,2:10}
        self.negative_labels = [
            "a social media logo",
            "a youtube logo",
            "a twitter logo",
        ]
        super().__init__()
    def default_value(self):
        return 0



    def get_endpoint(self):
        return "https://api-inference.huggingface.co/models/openai/clip-vit-base-patch32"

    def get_body(self, input,labels):
        # prep input for model
        data = {
            "inputs": input,
            "parameters": {
                "candidate_labels": labels
            },
            "wait_for_model": True,"use_cache":True
        }
        return data
    def get_labels(self,record_name,base_domain):
        return [f"the logo of {record_name}", f"the logo of this domain {base_domain}"] + self.negative_labels



    def visual_reasoning(self, img_link, label_1, label_2,results,threshold=65):
        positive_score = defaultdict(self.default_value)

        #positive labels

        positive_score[label_1] = 1

        positive_score[label_2] = 2


        input_labels = [i for i in positive_score.keys()] + self.negative_labels

        if isinstance(results,dict) and 'error' in  results.keys():

            #todo: Host clip on dedicated enpoint, in interim, dont reject if the model endpoint fails...
            return  Verdict(candidate=True, class_match=2, class_one=20,
                           class_two=20, diff_threshold=self.diff_map[2])


        top_score = results[0]['label']
        confidence = results[0]['score'] *100

        pos_one_score = [i['score'] for i in results if i['label']==input_labels[0]][0] *100
        pos_two_score = [i['score'] for i in results if i['label']==input_labels[1]][0]*100


        if positive_score[top_score] ==0:
            logging.info(f'image falls into the negative class: {img_link}')
            return Verdict(reject=True)

        max_index = positive_score[top_score]
        diff_threshold = self.diff_map[positive_score[top_score]]


        if confidence > threshold or (pos_one_score+pos_two_score) >threshold:
            if max_index == 1 or max_index == 2:

                return Verdict(output=True,class_match=max_index, class_one=pos_one_score,
                        class_two=pos_two_score,diff_threshold=diff_threshold)
        # make images 10 points below threshold as candidates
        if confidence >= (threshold -10) or (pos_one_score+pos_two_score) >(threshold -10):
            return Verdict(candidate=True, class_match=max_index, class_one=pos_one_score,
                           class_two=pos_two_score, diff_threshold=diff_threshold)
        return Verdict(reject=True)


class Verdict:
    def __init__(self, output=False,reject=False,candidate=False,class_match=None,class_one=0,class_two=0,diff_threshold=0):
        self.output = output
        self.reject = reject
        self.candidate  = candidate
        self.class_match = class_match
        self.class_one = class_one
        self.class_two = class_two
        self.diff_threshold = diff_threshold


