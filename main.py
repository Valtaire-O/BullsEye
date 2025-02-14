import functions_framework
from bullseye import BullsEye
from db_connect import BaseQuery
import uuid


def store_record(unique_id, name, url):
    data = {'stage_id': unique_id, 'record_name': name, 'url': url, 'image': 'n/a', "origin": 'endpoint'}
    BaseQuery().save_to_copy_db(data=[data])


def retrieve_record(stage_id):
    return BaseQuery().single_select_record(stage_id)


@functions_framework.http
def bully_function(request):
    """Start by ingesting records from DB,
    expects that all records will have hyperlinks"""

    request_json = request.get_json(silent=True)
    request_args = request.args
    if not request_json:
        return {"message": "Error! missing request body. "
                           "Expected format: {'url': 'any_url', 'name': 'descriptive name'"
                           "Note: name is optional"}
    if 'url' not in request_json:
        return {"message": "Error! missing key url, try adding 'url: any_url'"}

    url = request_json.get('url')
    if 'name' in request_json:
        name = request_json.get('name')
    else:
        name = ''  # todo if not name, grab og title downstream
    if 'current_logo' in request_json:
        current_logo = request_json.get('current_logo')
    else:
        current_logo = None

    """create unique id for record"""
    unique_id = uuid.uuid4()

    store_record(unique_id, name, url)

    bulls_eye = BullsEye(stage_id=unique_id, current_logo=current_logo)

    bulls_eye.main_site_extract()

    results = bulls_eye.find_logo()

    if current_logo:
        if len(results) == 4:
            image_url, similarity, og_image_url, message = results
            return {"url": url, "logo": image_url, "old_logo": current_logo, "og_image_url": og_image_url,
                    "similarity": similarity,
                    'message': message}
        else:
            print(f'error: results = {results}')
            message = {'success': 'no new logo found'}
            return {"url": url, "logo": "", "og_image_url": "", "old_logo": current_logo, "similarity": 0,
                    'message': message}
    else:
        if len(results) == 2:
            image_url, og_image_url = results
            return {"url": url, "logo": image_url, "og_image_url": og_image_url}
        else:
            print(f'error: results = {results}')
            return {"url": url, "logo": ""}