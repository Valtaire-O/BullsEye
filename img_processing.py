import re
from w3lib.html import remove_tags, replace_escape_chars
import pillow_avif
import matplotlib
from google.cloud import storage
import io
from  PIL import Image,ImageChops
import PIL
PIL.Image.MAX_IMAGE_PIXELS = 933120000
import colorsys
import math
from collections import Counter
import cv2
import numpy as np
from typing import Optional, List
import uuid
import logging

from time import perf_counter

class Preprocessor:
    """Process links before requesting them: confirming or disconfirming their validity"""
    def get_base_url(self,url):
        base = re.compile(r'^((?:https?:\/\/(?:www\.)?|www\.)[^\s\/]+\/?)').findall(url)
        if base:
            base = base[0]
            if base[-1] != '/':
                base +='/'

            return base
        return None

    def bad_prefix(self,url):
        return re.compile(r'^/|^//', flags=re.IGNORECASE).findall(url)

    def confirm_url(self,url: str, main_url=None):

        """Confirm the links are valid"""
        if url is None or type(url)!= str:
            return None

        if 'base64' in url:
            return None
        if 'data:image' in url:
            return None

        if '<' in url or '>' in url:
            return None
        no_spaces = url.strip().split(' ')

        if len(no_spaces) > 1:
            #There should be no spaces in the links, if there are, check to see if its a list of
            #valid urls and take the first one, if not see if its a domain and page link and join it
            get_url = [self.get_base_url(i) for i in no_spaces if self.get_base_url(i)]
            size =len(get_url)
            if size > 1:
                # indicates a 'list' of n urls
                url = no_spaces[0]

            elif size ==1:
                #indicates only the first url is valid
                url = ''.join(no_spaces)

            else:
                return None
        #if link is absolute, confirm its domain
        absolute = self.get_base_url(url)
        if absolute:
            valid_url = url.replace(absolute, absolute.lower())
            if re.compile(r'^www').findall(valid_url):
                return  'https://'+valid_url
            # make sure that the domain name has no uppercase, otherwise it will be changed by the browser
            return valid_url

        if main_url:
            #For relative paths, get the base url from the main url
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

                return None
            if re.compile(r'^www').findall(base_url):
                return  'https://'+base_url
            return base_url

        if not absolute and not main_url:
            new_url = 'https://' + url

            if self.confirm_url(new_url):
                return new_url
        return None

    def pre_processor(self,url):
        check_image_format = self.image_format(url)
        if check_image_format:
            check_stop_images = self.stop_images(url)
            if check_stop_images is True:
                return None
            return url
        return None

    def image_format(self, url):
        """match links with the following wanted formats"""
        if not url:
            return None
        image_types = re.compile(rf'(?i)(png|logo|jpg|svg|jpeg|avif|webp|image|ImageRepository)', flags=re.IGNORECASE).findall(url)
        if image_types:
            return url
        return None

    def stop_images(self,url):
        '''Reject all links matching unwanted patterns'''
        icon_re = 'icons?'
        social_re = '(facebook|socialmedia|social-media|instagram|snapchat|twitter|tiktok|reddit|pixel|blank)'
        social_icon = re.compile(rf'(?i)({icon_re}.*){social_re}|{social_re}(.*{icon_re})',
                                 flags=re.IGNORECASE).findall(url)
        if social_icon:
            return True

        social_subdomain = re.compile(rf'(?i){social_re}\.(png|svg|webp)', flags=re.IGNORECASE).findall(url)
        if social_subdomain and 'share' not in url:
            return True

        bad_gifs = re.compile(rf'(?i)(loading|loader|spacer|javascript|loading|blank\s*).*gif', flags=re.IGNORECASE).findall(url)
        if bad_gifs:
            return True
        if 'base64' in url:
            return True
        if 'storage.googleapis' in url and 'html' in url:
            return True
        return False



# python3 dags/dependencies/bulls_eye/img_processing.py


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
        """Find a blank image"""

        img = img.convert('RGB')

        #get all colors by getting max pixels H*W
        get_colors = img.getcolors(img.size[0] * img.size[1])
        #print(get_colors[0])
        #print(get_colors)


        if len(get_colors) == 1:
            #print('One color present this image is blank')
            return True


        if len(get_colors) <= 50:

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

                return True
        return False

    def check_transparency(self,img):
        '''Determine whether an image is transparent or not'''

        if img.mode == 'P' or img.mode == 'L':
            # P & L are possible modes for transparency, Convert to add Alpha Channel
            # and check intensity of the min pixel
            test = img.convert(img.mode + 'A')
            extrema = test.getextrema()

            # if the maximum instensity of the alpha chanel is 0, we have a transparent image
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



    def trim(self,im):
        bg = Image.new(im.mode, im.size, im.getpixel((0, 0)))
        diff = ImageChops.difference(im, bg)
        diff = ImageChops.add(diff, diff, 2.0, -100)
        # Bounding box given as a 4-tuple defining the left, upper, right, and lower pixel coordinates.
        # If the image is completely empty, this method returns None.
        bbox = diff.getbbox()
        if bbox:
            bg.close()
            return im.crop(bbox)
        else:
            bg.close()
            return im

    def open_img(self,response_content):
        try:
            # All Non images will fail here
            img = PIL.Image.open(response_content)
        except:
            return False
        return  img

    def convert_svg(self,content):
        import aspose.words as aw
        try:

            doc = aw.Document()
            builder = aw.DocumentBuilder(doc)

            shape = builder.insert_image(content)
            options = aw.saving.ImageSaveOptions(aw.SaveFormat.JPEG)

            # Calculate the maximum width and height and update page settings
            # to crop the document to fit the size of the pictures.
            pageSetup = builder.page_setup
            pageSetup.page_width = shape.width + 25
            pageSetup.page_height = shape.height + 25
            pageSetup.top_margin = 0
            pageSetup.left_margin = 0
            pageSetup.bottom_margin = 0
            pageSetup.right_margin = 0

            options.use_anti_aliasing = True
            options.use_high_quality_rendering = True
            img_bytes = io.BytesIO()
            doc.save(img_bytes, options)
            return self.open_img(img_bytes)
        except:
            return None



class Visibility:
    """Detect and reconcile low visibility of an image"""
    def detect_edges(self,img):
        """Create Edge map of image"""
        width = img.shape[1] + 2
        height = img.shape[0] + 2
        dim = (width, height)
        image= cv2.resize(img, dim, interpolation=cv2.INTER_AREA)
        # convert to grayscale and blur as a preprocessing measures
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        blurred = cv2.GaussianBlur(gray, (5, 5), 0)

        mid = cv2.Canny(blurred, 30, 150)


        #Get the values of the white border of the edge map
        indices = np.where(mid != [0])
        coordinates = ([i for i in zip(indices[0], indices[1])])
        return coordinates

    def ConvertFormat(self, imgObj, outputFormat=None):
        """take in an image and change its format"""
        newImgObj = imgObj
        if outputFormat and (imgObj.format != outputFormat):
            imageBytesIO = io.BytesIO()
            imgObj.save(imageBytesIO, outputFormat)
            newImgObj = Image.open(imageBytesIO)

        return newImgObj

    def paste_background(self,image,color,mode):
        """Add new background to transparent images"""
        image = image.convert('RGBA')
        img_w, img_h = image.size
        # Create a new image with roughly the same dimension plus padding
        back = Image.new("RGB", (img_w,img_h ), color=color)
        # needs to be in jpeg format in order to work
        back = self.ConvertFormat(back, outputFormat='JPEG')
        x = (back.width - img_w) // 2
        y = (back.height - img_h) // 2
        # Paste the existing image onto the new colored background at the center position
        back.paste(image, (x, y), image)
        return back

    def threshold_image(self,image):
        # Extract the alpha channel and threshold it at 90
        img = image.convert('RGBA')
        #img = img.resize((img.size[0] // 2, img.size[1] // 2), resample=Image.Resampling.LANCZOS)
        alpha = img.getchannel('A')
        alphaThresh = alpha.point(lambda p: 255 if p > 90 else 0)
        # Make a new completely black image same size as original
        res = Image.new('RGBA', img.size)

        # Copy across the alpha channel from original
        res.putalpha(alphaThresh)

        return res


    def get_colors(self,image,mode):
        """ Get the colors of a transparent image for the  true pixel count"""
        width,height = image.size
        pixel = image.load()
        true_colors = []

        for y in range(0, height):
            for x in range(0, width):
                color = pixel[x,y]
                if mode=='RGBA':
                    if color[3] > 90:
                        true_colors.append(color)

        return true_colors


    def pil_to_cv(self,img):
        """convert a pil image to cv2 image"""
        image = cv2.cvtColor(np.array(img.convert('RGB')), cv2.COLOR_RGB2BGR)
        return image

    def general_resize(self,img):
        '''Increase the size of an image if it is beneath the pixel threshold,
        but keep its dimensions'''
        pixel_threshold = 30000
        w,h = img.size
        area = w*h
        if area <pixel_threshold:
            #img.show()
            imageBytesIO = io.BytesIO()
            new_img = img.resize((w * 5, h * 5),resample=Image.NEAREST)

            return new_img
        return img


    def valgorithm(self,input_w,input_t,og_input,default=True,visible_edges=Counter,thresh_n=0,
                   gray_scale=(2,2,2)):
        """Compare the edge maps of a segmented image and the og image on a colored background,
        if the latter's edge map contains a sufficient amount of the edges found in the former,
        the image is visible, if not increase the intensity of the background color until it does"""
        visibility_threshold = 55
        max_h_sum = 192 #64 * 3
        visible_count =0
        visible_edges_start = visible_edges
        thresh_n_start = thresh_n
        input_g = og_input #input image with grayscale bckgrnd

        gs_color  =tuple([i * 2 for i in gray_scale])
        if default is True:
            visible_edges_start = Counter()
            #This is the initial comparison where the input image is pasted on a white background
            thresh_edge_map = self.detect_edges(input_t)

            input_edge_map = self.detect_edges(input_w)


            for x, y in thresh_edge_map:
                #Hash segmented image b(x,y) , threshold border pixels
                visible_edges_start[f"{x}:{y}"] = 1

            for x, y in input_edge_map:
                '''Check if the edges found in the white pasted input image map to the 
                    ones in the threshold image'''
                if visible_edges_start[f"{x}:{y}"] == 1:
                    visible_count += 1

            if len(thresh_edge_map) ==1:
                '''we've identified a bad image'''
                return {'img':og_input,'passed':False}
            thresh_n_start = len(thresh_edge_map)
            if visible_count ==0:
                percentage = 0
            else:
                percentage = round((visible_count / thresh_n_start) * 100)

            if percentage > visibility_threshold:
                final = self.paste_background(og_input, color='#FFFFFF', mode='RGB')
                return {'img':final,'passed':True}

        else:
            #If the image isn't fully visible on the  default background, paste a grayscale
            #background, starting with black, behind our image
            #repeat the  steps again, recursively increase the intensity of the background
            #by small increment h until
            #its fully visibility'''


            input_g = self.paste_background(og_input, color=gs_color, mode='RGB')

            input_g_map = self.detect_edges(self.pil_to_cv(input_g))

            for x, y in input_g_map:
                #Check if the edges found in the grayscale pasted input image map to the
                #ones in the threshold image
                if visible_edges[f"{x}:{y}"] == 1:
                    visible_count += 1

            if visible_count == 0:
                percentage = 0
            else:
                percentage = round((visible_count / thresh_n_start) * 100)


        if percentage <= visibility_threshold and sum(gs_color)<max_h_sum:
            #Make recursive call
            self.valgorithm(input_w,input_t,og_input,default=False,visible_edges=visible_edges_start,
                       thresh_n=thresh_n_start,gray_scale=gs_color)

        final = self.paste_background(og_input, color=gs_color, mode='RGB')
        return {'img':final,'passed':True}

    def check_visibility(self,img):
        """Using thresholding and Edge detection, see if the image is visible on
        some defined background color"""
        img = img.convert('RGBA')
        color_list = self.get_colors(img, 'RGBA')
        full_size = img.size[0] * img.size[1]

        cp = (len(color_list) / full_size) * 100
        if cp < 5:
            '''return passed:False'''
            pass

        img = self.general_resize(img)
        t_img = self.threshold_image(img)

        '''Create white pasted images of the  threshold and original input '''
        white_pasted_threshold = self.paste_background(t_img, color='#FFFFFF', mode='RGB')
        white_pasted_input = self.paste_background(img, color='#FFFFFF', mode='RGB')



        '''Convert to opencv for edge detection'''
        input_w = self.pil_to_cv(white_pasted_input)
        input_t = self.pil_to_cv(white_pasted_threshold)

        visibility = self.valgorithm(input_w, input_t, img)
        return visibility

class StartProcess():
    def __init__(self):

        self.post_processor = PostProcessor()
        self.visibility = Visibility()
        self.count = 0
        self.semi_visible = []
        self.transparent = []

    def stream_to_url(self,images, names):
        """Send in a byte stream, get a public image link back"""
        client = storage.Client('sunlit-shelter-377115')
        bucket = client.bucket('eco_one_images')


        if len(images) != len(names):
            raise ValueError("Images and names lists must be parallel.")

        pub_urls = []
        for (img, name) in zip(images, names):

            blob = bucket.blob(name)
            img.thumbnail((250,250), Image.Resampling.LANCZOS)
            img = img.convert('RGB')
            img_bytes = io.BytesIO()
            img.save(img_bytes, format='png',dpi=(300, 300))
            img_bytes = img_bytes
            blob.upload_from_file(img_bytes,content_type='image/png',rewind=True)

            pub_urls.append(blob.public_url)
        return pub_urls[0]




    def process_image(self, response_data: dict):
        """Expects image content bytes
        opens and imposes validity checks on the candidate images,
         Manipulating images as well. We return a dict where the
         image either passes or fails"""
        validate = Preprocessor()
        response_content = response_data["content"]
        content_type = response_data["content_type"]
        image_url = response_data["request_url"]

        passed = False
        modified = False
        if 'svg' in content_type or 'svg' in image_url:
            #img = self.post_processor.convert_svg(response_content)
            #modified = True
            return {"passed": False, 'modified': False, "image_url": image_url}


        img = self.post_processor.open_img(response_content=response_content)


        if img  and 'image' in content_type:
            #reject images that are too small
            '''l, w = img.size
            if (l*w)< 50000:
                return {"passed": False, 'modified': False, "image_url": image_url}'''

            # check for transparency
            "check & resolve Transparency"
            if 'png' in content_type or 'webp' in content_type:

                is_transparent = self.post_processor.check_transparency(img)


                if is_transparent:

                    visibility_check = self.visibility.check_visibility(img)
                    if visibility_check['passed'] is False:
                        img.close()
                        return {"passed": False, 'modified': False, "image_url": image_url}

                    modified = True
                    img.close()
                    img = visibility_check['img']


            """Check for blank images"""

            is_blank = self.post_processor.check_blanks(img)


            if is_blank:
                logging.info(f"Image  Failed Blank Test: {image_url}")

                img.close()
                return {"passed": False, 'modified': False, "image_url": image_url}

            # ensure all images coming out of processing are in rgb mode
            img = img.convert('RGB')
            if modified:
               """create new image link"""
               name_id = f"{uuid.uuid4()}"
               image_url = self.stream_to_url([img],[name_id])


            return {"passed": True, 'modified': modified, "image_url": image_url, 'img_obj':img}

        else:
            logging.info(f"Image  Failed to Open: {image_url}")
            return {"passed": False, 'modified': modified, "image_url": image_url}

#todo Replace the returned dict with a dataclass Object
