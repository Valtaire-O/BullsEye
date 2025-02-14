import torch
import numpy as np
import cv2
import math
from PIL import Image
import io
import textdistance as td
import torch.nn as nn
from typing import List, Dict
from google.cloud import vision
from sklearn.cluster import KMeans
import re
from cv_models import CVModels
from async_api import Asnyc_API
class Similarity:
    '''Base class for shared functionality'''

    def pil_to_cv(self, img):
        '''convert a pil image to cv2 image'''
        cv_img = cv2.cvtColor(np.array(img.convert('RGB')), cv2.COLOR_RGB2BGR)
        return cv_img

    def scale_image(self,img,color=False):
        length_x, width_y = img.size
        pixel_len = length_x * width_y
        new_img = img
        max_size = (640 ,480)
        if color:
            max_size = (100,100)
        l,w =max_size
        if pixel_len > l*w or pixel_len<l*w:
            new_img.thumbnail(max_size,Image.Resampling.LANCZOS)

        imageBytesIO = io.BytesIO()
        new_img.save(imageBytesIO,format='PNG', dpi=(300, 300))
        if color:
           return Image.open(imageBytesIO)

        return imageBytesIO.getvalue()


class ColorSimilarity(Similarity):
    '''Extract and compare the dominant colors of 2 images'''

    def most_dominant_color(self,input):
        #note, k means has a time complexity of  O(n k i),
        # as a result, images are downsampled before being processed
        img = input.convert('RGB')
        img = self.pil_to_cv(img)
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        #convert to np array for processing
        img = img.reshape((img.shape[0] * img.shape[1], 3))
        pix = np.asarray(img)

        # do one bin to take into account the mean of the most dominant colors
        kmeans = KMeans(n_init=6, n_clusters=1).fit(pix)
        # Get cluster center for largest cluster
        centroid = kmeans.cluster_centers_
        color = centroid.tolist()[0]
        rgb = [int(i) for i in color]
        return rgb

    def make_comparison(self, img_a, img_b):
        color_a = self.most_dominant_color(img_a)
        color_b = self.most_dominant_color(img_b)
        dist = self.compute_distance(color_a, color_b)
        return self.normalize_distance(dist)

    def compute_distance(self,current_color,new_color):
        '''Compute Euclidean Distance between 2 3d color channels'''
        distance = math.sqrt(sum([(current_color[i]-new_color[i])**2 for i in range(0,3)]))
        return int(distance)


    def normalize_distance(self,value):
        '''Normalize score to be between 0 & 1'''
        min = 0
        max= 441
        test = (value - min)/ max
        #reverse score to get dominant color similarity
        reversed_score = 1-test
        return reversed_score


class FeatureSimilarity():
    """Compares the general features of each image"""
    def __init__(self):
        self.cosine = nn.CosineSimilarity(dim=1)
        self.cv_interface = CVModels()
        self.async_calls = Asnyc_API()

    def make_comparison(self,img_a, img_b):
        #stringify raw image bytes for json
        endpoint = self.cv_interface.get_endpoint()
        headers = self.cv_interface.get_headers()

        extract_input = []
        #prepare data for for feature extraction
        for i in [img_a, img_b]:
            body = self.cv_interface.get_body(i,task='extract')
            extract_input.append({'model_endpoint':endpoint,'body':body})

        """make call to model endpoint via async API"""
        extract_output = self.async_calls.make_calls(extract_input, headers=headers,
                                        api_state=True)
        img_a_resp = extract_output[0]
        img_b_resp = extract_output[1]

        embed_list_a = img_a_resp['response']

        embed_list_b = img_b_resp['response']


        """pass errors downstream for client to suss out"""
        if not isinstance(embed_list_a, dict) or  not isinstance(embed_list_b,dict) :
            return None

        if embed_list_a['status'] =='error' or embed_list_b['status'] =='error':

            return None

        embed_list_a = embed_list_a['embedding']
        embed_list_b = embed_list_b['embedding']

        similarity = self.compute_cos_sim(embed_list_a, embed_list_b )

        return similarity

    def compute_cos_sim(self, embedding_a:List[float], embedding_b:List[float]):
        '''Compare images'''

        # convert embedding list back into tensor
        embedding_a = torch.Tensor(embedding_a)
        embedding_b = torch.Tensor(embedding_b)

        similarity = self.cosine(embedding_a, embedding_b)
        return similarity.item()



class OCR(Similarity):
    '''Extract and compare text found or not found on input images'''
    def __init__(self):
        self.client = vision.ImageAnnotatorClient()

    def extract_text(self,img):
        #send image to google vision API
        image = vision.Image(content=img)
        response = self.client.text_detection(image=image, image_context={"language_hints": ["en"]})
        # handle errors

        if response.error.message:
                return None

        texts = response.text_annotations
        if texts:
            # remove non asci chars
            text = re.sub(r'[^\x00-\x7f]', r'', texts[0].description).upper()
            return text.split()

        return []

    def make_comparison(self, img_a, img_b):

        img_a_text = self.extract_text(img_a)
        img_b_text = self.extract_text( img_b)

        #save cosine sim of text for later use
        #u,v = self.term_vectors(img_a_text,img_b_text)
        #similarity = td.cosine(u, v)
        if img_a_text == None or img_b_text ==None:
            return None

        if ' '.join(img_a_text).strip() == ' '.join(img_b_text).strip() :
            exact_match = 1
        else:
            exact_match = 0
        return  exact_match

    def term_vectors(self,u,v):
        '''Ensure vectors are of equal length before comparison'''
        u_len = len(u)
        v_len = len(v)
        #todo: fill smaller vector with placeholders more succinctly
        vector_map = {u_len:u,v_len:v}
        if u_len ==v_len:
            return u,v
        max_len = max([u_len,v_len])
        min_len = min([u_len,v_len])
        empty_values = ['' for i in range(0,max_len-min_len)]
        return vector_map[max_len], vector_map[min_len] + empty_values


class ComputeSimilarity(Similarity):
    def __init__(self):
        '''Compute the similarity of input images on N dimensions'''
        self.feature_similarity = FeatureSimilarity()
        self.text_similarity = OCR()
        self.color_similarity = ColorSimilarity()
        super().__init__()

    def compare_logos(self,current_logo,new_logo):
        img_a = current_logo
        img_b = new_logo

        #normalize size of image
        scaled_img_a = self.scale_image(img_a)
        scaled_img_b = self.scale_image(img_b)

        #both f_similarity and t_similarity use http endpoints for infernce , return comparison error
        #if the request fails

        feature_similarity = self.feature_similarity.make_comparison(img_a=scaled_img_a, img_b=scaled_img_b)
        if feature_similarity is None:
            return {'error':'feature extraction failed for one of the logos','stage':'comparison'}

        text_similarity = self.text_similarity.make_comparison(img_a=scaled_img_a, img_b=scaled_img_b)
        if text_similarity is None:
            return {'error':'text extraction failed for one of the logos','stage':'comparison'}

        color_similarity = self.color_similarity.make_comparison(img_a=self.scale_image(img_a,color=True),
                                                                 img_b=self.scale_image(img_b,color=True))
        #take the average of similarity scores
        scores = [feature_similarity, text_similarity, color_similarity]
        score_num = len(scores)
        score_sum = sum(scores)
        final_score = self.get_percentage(score_sum,score_num)

        return final_score

    def get_percentage(self,m:int,n:int):
        if m ==0 or n == 0:
            return 0
        return (m/n) *100

