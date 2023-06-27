import os
import re
from w3lib.html import remove_tags, replace_escape_chars
import json
import pillow_avif
import matplotlib
import io
from  PIL import Image
import PIL
import colorsys
import math
from collections import Counter

import torch
import glob
from torchvision.models import resnet101
from torchvision.transforms import transforms
from torchvision.models import ResNet101_Weights

from torch.nn.functional import cosine_similarity
import torch.nn as nn
from pathlib import Path


class Preprocessor:
    def get_base_url(self,url):
        base = re.compile(r'^((?:https?:\/\/(?:www\.)?|www\.)[^\s\/]+\/?)').findall(url)
        if base:
            return base[0]
        return None

    def bad_prefix(self,url):
        return re.compile(r'^/|^//', flags=re.IGNORECASE).findall(url)

    def confirm_url(self,url: str, main_url=None):

        """Confirm the links are valid"""
        if url is None or type(url)!= str:
            return None

        if 'base64' in url:
            return None
        no_spaces = url.strip().split(' ')

        if len(no_spaces) > 1:
            """There should be no spaces in the links, if there are, check to see if its a list of 
            valid urls and take the first one, if not see if its a domain and page link and join it"""
            get_url = [self.get_base_url(i) for i in no_spaces if self.get_base_url(i)]
            size =len(get_url)
            if size > 1:
                # indicates a 'list' of n urls
                url = no_spaces[0]

            elif size ==1:
                'indicates only the first url is valid'
                url = ''.join(no_spaces)

            else:
                return None
        """if link is absolute, confirm its domain"""
        absolute = self.get_base_url(url)
        if absolute:
            valid_url = url.replace(absolute, absolute.lower())
            if re.compile(r'^www').findall(valid_url):
                return  'https://'+valid_url
            # make sure that the domain name has no uppercase, otherwise it will be changed by the browser
            return valid_url

        if main_url:
            """For relative paths, get the base url from the main url"""
            base_url = self.get_base_url(main_url)
            # make sure theres no slashes at the start of the path
            if self.bad_prefix(url):
                new_url = url.split('/')
                clean_url = [i for i in new_url if i]
                # if the base url has no slash add one to make a valid url
                if base_url[-1] != '/':
                    base_url = base_url + '/' + '/'.join(clean_url)
                else:
                    base_url = base_url + '/'.join(clean_url)

            else:
                base_url = base_url + url
            no_spaces = replace_escape_chars(base_url).strip().split(' ')

            if len(no_spaces) > 1:
                # print(base_url)
                return None
            if re.compile(r'^www').findall(base_url):
                return  'https://'+base_url
            return base_url
    def pre_processor(self,url):
        check_image_format = self.image_format(url)
        if check_image_format:
            check_stop_images = self.stop_images(url)
            if check_stop_images is True:
                return None
            return url
        return None

    def image_format(self, url):
        if not url:
            return None
        image_types = re.compile(rf'(?i)(png|logo|jpg|svg|jpeg|gif|webp|image)', flags=re.IGNORECASE).findall(url)
        if image_types:
            return url
        return None

    def stop_images(self,url):
        '''Reject all links matching stop image patterns'''
        icon_re = 'icons?'
        social_re = '(facebook|socialmedia|social-media|instagram|snapchat|twitter|tiktok|reddit)'
        social_icon = re.compile(rf'(?i)({icon_re}.*){social_re}|{social_re}(.*{icon_re})',
                                 flags=re.IGNORECASE).findall(url)
        if social_icon:
            return True

        social_subdomain = re.compile(rf'(?i){social_re}\.(png|svg|webp)', flags=re.IGNORECASE).findall(url)
        if social_subdomain and 'share' not in url:
            return True

        bad_gifs = re.compile(rf'(?i)(loading|loader|spacer|javascript).*gif', flags=re.IGNORECASE).findall(url)
        if bad_gifs:
            return True
        if 'base64' in url:
            return True
        return False

class TorchImage:
    def __init__(self):

        # Load ResNet101
        self.model = resnet101(weights=ResNet101_Weights.DEFAULT)
        self.model = self.model.eval()

        # Remove last layer (classifier) to get embeddings instead of class probabilities
        self.model = torch.nn.Sequential(*list(self.model.children())[:-1])

        # Define transformations
        self.transform = transforms.Compose(
                [   #seems to work better at 224 vs 256
                    transforms.Resize(224),
                    transforms.ToTensor(),
                    transforms.Normalize(
                        mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                    ),
                ]
            )


        # Directory with images
        self.image_dir = 'bulls_eye/stop_images/warnings'
        self.image_files =glob.glob(f'{self.image_dir}/*')

        self.image_dir = glob.glob(f'bulls_eye/stop_images/*')
        self.images_by_dir = {}
        self.embeddings = {}

        #skim embeddings gets the embeding of the first image of every dir
        self.skim_embeddings ={}

        #Each Dir will hold a dict of embeddings for each of its files
        for f in self.image_dir:
            self.embeddings[f] = {}
            images = glob.glob(f'{f}/*')
            self.images_by_dir[f] = images


        self.store_embeddings()



    def embed_image(self,img):
        new_img_t = self.transform(img)
        new_batch_t = torch.unsqueeze(new_img_t, 0)
        with torch.no_grad():
            embedding = self.model(new_batch_t)
        return embedding


    def store_embeddings(self):
        # Store embeddings for all images in a dictionary
        for dr in self.images_by_dir:
            count = 0
            for image_file in self.images_by_dir[dr]:
                count +=1
                img = Image.open(image_file).convert('RGB')
                embedding = self.embed_image(img)

                self.embeddings[dr][image_file] = embedding
                if count ==1:
                    self.skim_embeddings[dr] =  embedding



    # Function to check similarity of new image to existing images
    def compare_new_image(self, img,threshold=0.57):
        """Compare each image to one stop image from each dir"""
        # Load and transform new image
        new_img = img.convert('RGB')
        new_embedding = self.embed_image(new_img)
        # Compare to existing images

        cosine = nn.CosineSimilarity(dim=1)

        for image_dir, embedding in self.skim_embeddings.items():
            similarity = cosine(new_embedding, embedding)
            if similarity.item() > threshold:

                # If an image is similar to one of the stop images, test it against the directory
                dir_comp = self.compare_to_dir(new_embedding,dr=image_dir)
                if dir_comp:
                    new_img.show()
                    return True
        return False




        #return print(f"The image {Path(new_image_file).name} is dissimilar to the corpus.")
    def compare_to_dir(self,new_embedding,dr,threshold=0.63):
        avg_sim = []
        cosine = nn.CosineSimilarity(dim=1)

        for image_file, embedding in self.embeddings[dr].items():
            similarity = cosine(new_embedding, embedding)
            avg_sim.append(similarity.item())

        average = sum(avg_sim) / len(avg_sim)

        if average >= threshold:
            print(f"average similarity to {dr.split('/')[-1]} image class: {average}")
            return True
        return False

    def validate(self, testing_img, threshold=0.63):
        # Load and transform new image

        new_img = Image.open(testing_img).convert('RGB')

        new_img_t = self.transform(new_img)
        new_batch_t = torch.unsqueeze(new_img_t, 0)

        with torch.no_grad():
            new_embedding = self.model(new_batch_t)

        # Compare to existing images
        avg_sim = []
        cosine = nn.CosineSimilarity(dim=1)
        dis_sim = False
        for image_file, embedding in self.embeddings.items():

            if image_file.split('/')[-1] != testing_img.split('/')[-1]:
                similarity = cosine(new_embedding, embedding)

                avg_sim.append(similarity.item())


        average = sum(avg_sim) / len(avg_sim)
        if average < threshold:
            new_img.show()
        print(average)
        return average, dis_sim


'''image_cosine = TorchImage()
#image_cosine.compare_new_image('bulls_eye/test_images/not_even_clos.jpeg')
file =  glob.glob(f'bulls_eye/test_images/*')

for f in file:
    print(f"Testing : {f}")
    image_cosine.compare_new_image(f)
    print('')'''


'''file =  glob.glob(f'bulls_eye/stop_images/warnings/*')
out = []
for f in file:
    print(f"Testing : {f}")
    avg = image_cosine.validate(f)



    out.append(avg)
out.sort()
print(out)'''




# python3 bulls_eye/shared_classes.py
class PostProcessor:
    def get_dif(self,v):
        v.sort(reverse=True)
        k = v.pop(0)
        return [k - i for i in v]

    def avg_difference(self,matrix):
        '''Take in a matrix of unique rgb combo's, get the avg difference between all colors
         '''
        r_vector = []
        g_vector = []
        b_vector = []
        for r, g, b in matrix:
            r_vector.append(r)
            g_vector.append(g)
            b_vector.append(b)

        r_v = self.get_dif(r_vector)
        r_dfv = sum(r_v) / len(r_v)

        g_v = self.get_dif(g_vector)
        g_dfv = sum(g_v) / len(g_v)

        b_v = self.get_dif(b_vector)
        b_dfv = sum(b_v) / len(b_v)
        avg_dif = sum([r_dfv, g_dfv, b_dfv]) / len([r_dfv, g_dfv, b_dfv])
        return avg_dif


    def check_blanks(self,img):
        """Ensures that the link is an image and that it is not empty!"""
        print('Checking for Blanks')
        img = img.convert('RGB')
        #get max colors by getting max pixels H*W
        get_colors = img.getcolors(img.size[0] * img.size[1])
        # print(get_colors)

        if len(get_colors) == 1:
            #print('One color present this image is blank')
            return True

        if len(get_colors) <= 5:
            #print('Less than or 5 colors present')
            # get the HSL values of the RGB values
            values = [colorsys.rgb_to_hls(i[1][0] / 255 * 100, i[1][1] / 255 * 100, i[1][2] / 255 * 100) for i in
                      get_colors]
            matrix = []
            """calculate the avg difference between the colors , less than 1 counts as blank"""
            for v in values:
                v = [math.trunc(i) for i in v]
                matrix.append(v)

            avg_dif = self.avg_difference(matrix=matrix)

            if avg_dif < 1:

                print('this image is blank')
                return True

        return False

    def check_transparency(self,img):
        '''Determine whether an image is transparent or not'''

        if img.mode == 'P' or img.mode == 'L':
            # P & L are possible modes for transparency, Convert to add Alpha Channel
            # and check intensity of the min pixel
            test = img.convert(img.mode + 'A')
            extrema = test.getextrema()

            #print(f' P&L chk img  extrema:{extrema}')
            if extrema[-1][0] == 0:

                return True

            return False

        elif img.mode[-1] == "A":
            extrema = img.getextrema()
            # checks if the alpha channel's minimum is below 255
            #print(f'img  extrema:{extrema}')
            alpha_channel = extrema[-1]
            if alpha_channel[0] ==0 and alpha_channel[1] ==0:
                #print(f'image:  is Completley Transparent\n')
                return False
            if alpha_channel[0] ==0:

                return True

            return False

        return False

    def check_for_whiteness(self,img):
        """check for white in a transparent image"""
        img = img.convert('RGBA')
        get_colors = img.getcolors(img.size[0] * img.size[1])

        whiteness = Counter()
        for i in get_colors:
            if sum(i[1]) >= 1000:
                whiteness[1000] = i[0]
                return True
        return False

    def open_img(self,response_content):
        try:
            # All Non images will fail here ( and SVG's)
            img = PIL.Image.open(response_content)
        except:

            return False
        return  img
        # check for transparency

    def ConvertFormat(self,imgObj, outputFormat=None):
        """take in an image and change its format"""
        newImgObj = imgObj
        if outputFormat and (imgObj.format != outputFormat):
            imageBytesIO = io.BytesIO()
            imgObj.save(imageBytesIO, outputFormat)
            newImgObj = Image.open(imageBytesIO)

        return newImgObj

    def resolve_transparency(self,image, background_color):
        image = image.convert('RGBA')
        img_w, img_h = image.size

        # Create a new image with roughly the same dimension plus padding
        back = Image.new("RGB", (round(img_w * 1.7), round(img_h * 1.7)), color=background_color)
        # needs to be in jpeg format in order to work
        back = self.ConvertFormat(back, outputFormat='JPEG')

        x = (back.width - img_w) // 2
        y = (back.height - img_h) // 2

        # Paste the existing image onto the new colored background at the center position
        back.paste(image, (x, y), image)
        return back






