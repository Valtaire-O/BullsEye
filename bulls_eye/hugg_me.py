import pillow_avif
import matplotlib
from PIL import Image
import PIL
import io
import glob


def ConvertFormat(imgObj, outputFormat=None):
    """take in an imahe and change its format"""
    newImgObj = imgObj
    if outputFormat and (imgObj.format != outputFormat):
        imageBytesIO = io.BytesIO()
        imgObj.save(imageBytesIO, outputFormat)
        newImgObj = Image.open(imageBytesIO)

    return newImgObj


def resolve_transparency(image, background_color):
    img_w, img_h = image.size

    # Create a new image with roughly the same dimension plus padding
    image = image.convert('RGBA')
    back = Image.new("RGB", (round(img_w * 1.7), round(img_h * 1.7)), color=background_color)
    # needs to be in jpeg format in order to work
    back = ConvertFormat(back, outputFormat='JPEG')

    x = (back.width - img_w) // 2
    y = (back.height - img_h) // 2

    # Paste the existing image onto the new colored background at the center position
    back.paste(image, (x, y), image)
    # back.show()
    return back


import json
from img_processing import PostProcessor, TorchImage
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse

'''lwith open('/Users/valentineokundaye/PycharmProjects/BullsEye/bulls_eye/bulls_eye/spiders/image_data/img_response_phase.json') as f:
    found_images = json.load(f)


with open('/Users/valentineokundaye/PycharmProjects/BullsEye/bulls_eye/bulls_eye/spiders/site_data/site_response_phase.json') as f:
    site_phase = json.load(f)

all_data = found_images['found'] + found_images['none_found']
print(len(all_data))
print(len(set([i['id'] for i in all_data])))'''

with open('bulls_eye/bulls_eye/spiders/test_links/transparent.json') as f:
    transparent = json.load(f)

# test_images = test_images[200:]


post_processor = PostProcessor()
# torch_vision = TorchImage()
scrapfly = ScrapflyClient(key='e5e77e603ae744b0aa408e4f0b59cd61', max_concurrency=20)
count = 0

'''hits = []
for i in transparent [:100]:
    count +=1
    with scrapfly as scraper:
        try:
            response = scraper.scrape(ScrapeConfig(url=i, retry=False, country='us'))
            status =response.status_code
            content_type = response.headers.get('content-type')
        except:
            continue
    img = post_processor.open_img(response_content=response.content)
                    #print(img)
    if img and status==200 :
        # check for transparency
        if 'png' in i or 'webp' in i:
            """For Formats check for transparency, then check for white colored pixels"""
            is_transparent = post_processor.check_transparency(img)
            if is_transparent:

                has_white = post_processor.check_for_whiteness(img)
                if has_white:
                    # todo: Currently hardcoding the background colors based on
                    #  whiteness, later will pass in color of main site header

                    img = post_processor.resolve_transparency(img, background_color='#000000')
                else:
                    img = post_processor.resolve_transparency(img, background_color='#FFFFFF')

        is_blank = post_processor.check_blanks(img)
        if is_blank:
            blank = True
            print(f"Blank  Image: {i}")

        else:
            stop_image = torch_vision.validate(img)
            if stop_image:
                #img.show()
                print(f'image: {i} failed stop check')
                hits.append(i)
            else:
                print(f'image: {i} passed stop check')

js = open('hits.json', "w")
js.write(json.dumps({"Row": hits }, indent=2, sort_keys=True))
js.close()'''


def test_transparency():
    for i in transparent['Row']:
        image = i['image']
        color = i['color']

        with scrapfly as scraper:
            try:
                response = scraper.scrape(ScrapeConfig(url=image, retry=False, country='us'))
                status = response.status_code
                content_type = response.headers.get('content-type')
            except:
                continue
        img = post_processor.open_img(response_content=response.content)
        new_img = resolve_transparency(img, color)

        img.show()
        new_img.show()
        print(i)
    return 0


def test_stops():
    image_cosine = TorchImage()

    # image_cosine.compare_new_image('bulls_eye/test_images/not_even_clos.jpeg')
    file = glob.glob(f'bulls_eye/test_images/*')

    for f in file:
        print(f"Testing : {f}")
        img = Image.open(f)
        image_cosine.compare_new_image(img)
        print('')
    return 0
test_stops()
# test_transparency()
# python3 bulls_eye/hugg_me.py