import sys
import sqlalchemy
import json
from collections import Counter
from sqlalchemy.orm import Session
from google.cloud import secretmanager
import ast
from img_processing import Preprocessor
from typing import Optional, List,Dict
class CloudConnect:
    def __init__(self):
        self.version_id = '/versions/latest'
        self.project_id = 'projects/906425369069/secrets/'
        self.cloud_path = 'lib/python3.10/site-packages/working_directory/root/'
        self.production = True

    def access_secret_version(self, secret_version_id):
        """Return the value of a secret credential"""

        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Access the secret version.
        response = client.access_secret_version(name=secret_version_id)

        # Return the decoded payload.
        return response.payload.data.decode('UTF-8')

    def connect_tcp_socket(self) -> sqlalchemy.engine.base.Engine:
        """ Initializes a TCP connection pool for a Cloud SQL instance of MySQL. """
        if self.production is False:
            password = "Cwodp3eateL"
            host = "localhost"
            user = "root"
            port = "3306"
            db_name = "bullydb"
        else:
            #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')


            password = self.access_secret_version(f"{self.project_id}db_password{self.version_id}")
            host = self.access_secret_version(f"{self.project_id}db_host{self.version_id}")
            user = self.access_secret_version(f"{self.project_id}db_user{self.version_id}")
            db_name = "bullydb"
            port = self.access_secret_version(f"{self.project_id}db_port{self.version_id}")

        connect_args = {}
        # For deployments that connect directly to a Cloud SQL instance without
        # using the Cloud SQL Proxy, configuring SSL certificates will ensure the
        # connection is encrypted.

        # [START cloud_sql_mysql_sqlalchemy_connect_tcp]
        pool = sqlalchemy.create_engine(
            # Equivalent URL:
            # mysql+pymysql://<valentine>:<db_pass>@<localhost>:<3306>/<db_name>
            sqlalchemy.engine.url.URL.create(
                drivername="mysql+pymysql",
                username=user,
                password=password,
                host=host,
                port=port,
                database=db_name,
            ),

            # [START cloud_sql_mysql_sqlalchemy_limit]
            # Pool size is the maximum number of permanent connections to keep.
            pool_size=5,
            # Temporarily exceeds the set pool_size if no connections are available.
            max_overflow=2,
            # The total number of concurrent connections for your application will be
            # a total of pool_size and max_overflow.
            pool_timeout=90,  # 30 seconds

            # 'pool_recycle' is the maximum number of seconds a connection can persist.
            # Connections that live longer than the specified amount of time will be
            # re-established
            pool_recycle=1800,  # 30 minutes

        )
        return pool

    def get_slack_key(self, testing=False):
        if testing:
            return self.access_secret_version(f"{self.project_id}"
                                              f"slack_testspace{self.version_id}")
        return self.access_secret_version(f"{self.project_id}slack_workspace{self.version_id}")

    def get_scraping_api_key(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"scraping_api_key{self.version_id}")
    def get_model_api_key(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"hugging_face_vl{self.version_id}")
    def get_eco_endpoint(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"eco_endpoint{self.version_id}")
    def get_eco_token(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"eco_token{self.version_id}")
    def get_staging_url(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"SUPABASE_URL{self.version_id}")
    def get_staging_key(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"SUPABASE_KEY{self.version_id}")
class BaseQuery:
    """This class constructs the building blocks for db queries used throughout the code base"""
    def __init__(self, testing=False,dci=False):
        self.cloud_conn = CloudConnect()
        self.curr = self.cloud_conn.connect_tcp_socket().connect()
        self.config = Session(self.curr)
        self.current_records = Counter()
        self.stage_tb = 'crawls'
        self.bully_storage = 'start_set'
        self.storage_aux_table = 'site_data'
        self.image_phase = 'image_discovery'
        self.site_phase = "site_extract"
        self.api_url = 'https://api.scrapfly.io/scrape?url='
        self.params = '&country=us&asp=True'
        self.resident = '&proxy_pool=public_residential_pool'
        self.scrape_key = f'&key={self.cloud_conn.get_scraping_api_key()}'
        self.validate = Preprocessor()



        if testing is True:
            self.stage_tb = 'image_testing'


        #get copy storage table columns {self.bully_storage}
        #self.curr.execute(f"""select * from {self.bully_storage}""")



        '''self.keys = [i[0] for i in self.get_columns[1:]]
        self.query_columns = self.column_format(self.keys)

        self.url = self.cloud_conn.get_staging_url()
        self.key = self.cloud_conn.get_staging_key()

        self.supabase: Client = create_client(self.url, self.key)'''


    def get_current_records(self,dci=False):
        """see if a record already exists in the db"""

        records = self.curr.execute(f"""select stage_id from {self.bully_storage}""")
        for r in records.fetchall():
            self.current_records[f"{r[0]}"] = 1
        return 0

    def column_format(self, value: list) -> str:
        '''transform list of strings into format suitable for db query'''

        # removes outer brackets and string quotations, leaving just the column names and commas
        return str(value)[1:-1].replace("'", ' ')
    def get_curr(self):
        return self.cloud_conn.connect_tcp_socket().connect()
    def get_config(self):
        return Session(self.curr)
    def save_to_copy_db(self, data:List[Dict]):
        self.get_current_records()

        for d in data:
            stage_id = d['stage_id']
            image = d['image']


            """Either update record if it already exists in the db, or insert it"""
            if self.current_records[f"{stage_id}"] != 0:
                self.curr.execute(f"""update {self.bully_storage}
                                      set done=False, `found`=False,needs_patch=True,image=%s
                                      where stage_id ='{stage_id}'
                                                   """,(image))
                self.curr.execute(f"""update {self.storage_aux_table}
                                                      set phase='{self.site_phase}'
                                                      where stage_id ='{stage_id}'
                                                                   """)

            else:
                self.curr.execute(
                    f"""INSERT INTO {self.bully_storage} (record_name, image, main_url, stage_id, needs_patch, `found`, done,origin)
                    Values( %s,%s,%s,%s,%s,%s,%s,%s) """,
                    (
                        d["record_name"],
                        d["image"],
                        d["url"],
                        d["stage_id"],
                        1,
                        0,
                        0,
                        d["origin"]
                    ))
                self.curr.execute(
                    f"""INSERT INTO {self.storage_aux_table} (stage_id, possible_images,
                     meta_image, phase,verification_fix)
                                    Values( %s,%s,%s,%s,%s) """,
                    (
                        d["stage_id"],
                        f"{[]}",
                        "n/a",
                        "site_extract",
                        'n/a'

                    ))
        self.config.commit()

    def site_extract_data(self,stage_id=False):
        main_query = f""" select record_name , image ,main_url , needs_patch , `found` ,  done ,  origin, start_set.stage_id  from {self.bully_storage}
                         inner join {self.storage_aux_table}
                         on {self.storage_aux_table}.stage_id  = {self.bully_storage}.stage_id
                         where done=False and image='n/a'  and phase='{self.site_phase}'"""
        if stage_id:
            add_on = f"""and {self.storage_aux_table}.stage_id = '{stage_id}'"""
            main_query += add_on

        data = self.curr.execute(main_query)


        data = data.fetchall()
        start_data = []
        keys = ["record_name" , "image" ,"main_url" , "needs_patch" , "found" ,  "done" ,  "origin" ,"stage_id"]
        for d in data:

            record = dict(zip(keys, d))
            url = record["main_url"]
            if '.gov' in url:
                record['endpoint'] = self.api_url + record["main_url"] + self.params+self.resident + self.scrape_key
            else:

                record['endpoint'] =   self.api_url + record["main_url"] + self.params + self.scrape_key
            record['base_url'] = self.validate.get_base_url(record['main_url'])
            start_data.append(record)

        return start_data

    def image_discovery_data(self,stage_id=False):
        """select image,main_url,possible_images,meta_image,color ,dci_start_set.stage_id from dci_start_set
            inner join dci_site_data
            on dci_start_set.stage_id  = dci_site_data.stage_id
            where  done=False  and image = 'n/a' and possible_images !='[]' and phase ='image_discovery';"""
        main_query = f""" select record_name, image, main_url,possible_images,meta_image,{self.bully_storage}.stage_id  from {self.storage_aux_table} 
            inner join {self.bully_storage}
            on {self.storage_aux_table}.stage_id  = {self.bully_storage}.stage_id
            where done=False and image='n/a' 
            and possible_images !='[]' and phase ='{self.image_phase}' """
        add_on = f"""and {self.storage_aux_table}.stage_id = '{stage_id}' """
        if stage_id:

            main_query+=add_on


        found_data = self.curr.execute(main_query).fetchall()

        keys = ["record_name", "image", "main_url", "possible_images", "meta_image", "stage_id"]


        found_images = []
        endpoint_map = Counter()


        for d in found_data:
            record = dict(zip(keys, d))
            endpoint_arr = []

            record['base_url'] = self.validate.get_base_url(record['main_url'])
            record['possible_images'] = ast.literal_eval(record['possible_images'])
            '''if record['meta_image']:
                endpoint_arr.append(record['meta_image'])'''

            for p in record['possible_images']:


                endpoint_arr.append(p)
                #endpoint_arr.append(wrap_url)
            # last item is the backup
            record['endpoint'] = endpoint_arr.pop(0)
            record['endpoint_arr'] = endpoint_arr
            record['endpoint_arr'].append('https://logo.clearbit.com/'+ record['base_url'])
            record['old_be_logo'] = d['image']
            found_images.append(record)
        secondary_query = f""" select record_name, image, main_url,possible_images,meta_image,{self.bully_storage}.stage_id  from {self.storage_aux_table} 
                    inner join {self.bully_storage}
                    on {self.storage_aux_table}.stage_id  = {self.bully_storage}.stage_id
                    where done=False and image='n/a' 
                    and possible_images='[]' and phase ='{self.image_phase}'"""


        if stage_id:
            secondary_query += add_on
        none_found_data = self.curr.execute(secondary_query).fetchall()
        no_images = []
        for d in none_found_data:

            record = dict(zip(keys, d))
            record['base_url'] = self.validate.get_base_url(record['main_url'])

            if not record['base_url']:
                continue

            record['endpoint'] = 'https://logo.clearbit.com/'+ record['base_url']

            record['possible_images'] = []
            record['endpoint_arr'] =[]
            record['old_be_logo'] = d['image']
            #record['possible_images'] = ast.literal_eval(record['possible_images'])
            no_images.append(record)
        return found_images+no_images

    def image_discovery_data_single(self,stage_id):

        main_query = f""" select record_name, image, main_url,possible_images,meta_image,{self.bully_storage}.stage_id  from {self.storage_aux_table} 
            inner join {self.bully_storage}
            on {self.storage_aux_table}.stage_id  = {self.bully_storage}.stage_id
            where done=False and image='n/a' 
            and possible_images !='[]' and phase ='{self.image_phase}' and {self.storage_aux_table}.stage_id = '{stage_id}' """

        record_data = self.curr.execute(main_query).fetchall()
        keys = ["record_name", "image", "main_url", "possible_images", "meta_image", "stage_id"]


        record_candidates = []

        if  record_data:
            found_data = record_data[0]
            record = dict(zip(keys, found_data))
            record_name= record['record_name']
            image= record['image']
            main_url = record['main_url']
            base_domain = self.validate.get_base_url(record['main_url'])

            meta_image = record['meta_image']
            possible_images = ast.literal_eval(record['possible_images']) + ['https://logo.clearbit.com/'+ base_domain]


            for i in possible_images:
                endpoint = i
                record_candidates.append({"record_name":record_name,"image":image,
                                              "main_url":main_url,"meta_image":meta_image,
                                              "endpoint":endpoint,"stage_id":stage_id,'base_domain':base_domain
                                              })

            return record_candidates

        return []




    def add_site_data(self,records,found:bool):
        """after scraping site add og image, list of possible images """
        for r in records:
            stage_id = r['stage_id']
            if found is True:
                meta_image = r['meta_image']
                possible_images = f"{r['possible_images']}"# stringified list
            else:
                meta_image = ''
                possible_images = f"{[]}"

            self.curr.execute(f"""update {self.storage_aux_table}
                                                      set possible_images= %s, `meta_image`=%s,
                                                       phase ='{self.image_phase}'
                                                      where stage_id ='{stage_id}'
                                                                   """, (possible_images,meta_image) )

        self.config.commit()



    def test_links(self):

        data = self.curr.execute(f""" select image  from start_set  where image!='n/a'""").fetchall()

        start_data = []

        for d in data:
            record = {"image":d[0]}

            start_data.append(record)
        js = open("testing_images.json", "w")
        js.write(json.dumps({"Row": start_data}, indent=2, sort_keys=True))
        js.close()

        return start_data

    def update_found(self, records, verification: bool):
        for r in records:
            stage_id = r['stage_id']
            needs_patch = True
            output = r['output_image']
            # print(f"This is the image:{image}, {record}")
            if verification is True:
                #Todo need to patch bool column for staging
                """If the image is valid no need to patch it"""
                self.curr.execute(
                    f"""update {self.bully_storage} set found=True,done=True, needs_patch=False where stage_id='{stage_id}'""",
                   )

                # if an image has been verified, no need to re patch
                # later we may want to patch whatever column that may be apart of a trigger
            else:

                self.curr.execute(
                    f"""update {self.bully_storage} set found=True,done=True, needs_patch=True, image= %s where stage_id='{stage_id}'""",
                    (output))
        self.config.commit()
        return 0

    def update_none_found(self, records, verification: bool):
        for r in records:

            stage_id = r['stage_id']

            none_found = 'n/a'
            if verification is True:
                """if image fails verification, set it up for site extraction"""
                issue = r['verification_fix']
                self.curr.execute(
                    f"""update {self.bully_storage}
                    inner join {self.storage_aux_table}
                     on {self.bully_storage}.stage_id = {self.storage_aux_table}.stage_id
                     set image='{none_found}',  `found` =False, done=False,verification_fix='{issue}'  where {self.bully_storage}.stage_id = '{stage_id}'""")
                self.config.commit()
            else:
                """If image fails during discovery set it up for patching"""

                self.curr.execute(
                    f"""update {self.bully_storage}
                     inner join {self.storage_aux_table}
                     on {self.bully_storage}.stage_id ={self.storage_aux_table}.stage_id
                     set image='{none_found}', done=True, `found`=False, needs_patch=True where {self.bully_storage}.stage_id='{stage_id}'""",)
        self.config.commit()

    def update_live_staging(self):
        data = self.select_processed_records(origin='staging')
        found_data = data[0]

        none_found_data = data[1]
        #check_set = self.supabase.table('ravens_table').select("id",'url').eq("image_check",False).execute()


        count = 0
        for f in found_data:
            stage_id = f['stage_id']
            
            count +=1
            image = f['image']
            #print(image)
            self.supabase.table('prod_data').update({"image": image}).eq("id",stage_id).execute()

        '''for n in none_found_data:
            
            stage_id = n['stage_id']
            image = n['image']
            self.supabase.table(self.stage_tb).update({"image_check": True}).eq("id", stage_id).execute()'''
        all_updates = none_found_data + found_data
        # make updates to done records
        #print(count)

        self.update_checked_records(all_updates)


    def update_checked_records(self,data):
        #data = self.select_processed_records('staging')
        #records = data[0] +data[1]
        for d in data:
            stage_id = d['stage_id']
            self.curr.execute(
                f"""update start_set set done=True, needs_patch = false where stage_id = '{stage_id}'""")
        self.config.commit()

    def select_processed_records(self,origin):
        keys = ["stage_id", "image"]
        found_data = self.curr.execute(
            f""" select stage_id, image  from start_set where done=True and 
            found=True and needs_patch =True and image!='n/a' and origin='{origin}'""").fetchall()

        none_found_data = self.curr.execute(
            f""" select stage_id, image  from start_set where done=True
             and found=False and needs_patch =True and image='n/a' and origin='{origin}'""").fetchall()


        found, none_found = [], []

        for f in found_data:
            found.append(dict(zip(keys, f)))

        for nf in none_found_data:
            none_found.append(dict(zip(keys, nf)))

        return found, none_found

    def test_set(self):
        self.curr.execute(f""" select main_url  from start_set """).fetchall()

    def select_live_staging(self):
        # selects records where image is empty and image_check is False
        #data_set = self.supabase.table(self.stage_tb).select("id", "name", "url","on_ecosystems").eq("image_check",False).execute()
        data_set = self.supabase.table('prod_data').select("id", "name", "url","ecosystems",'image','bubble_id').execute()
        validate = Preprocessor()
        new_data = []
        bad_count = 0
        eco_list = 'Collectibles - Sports'
        for d in data_set.data:
            confirm_url = validate.confirm_url(d['url'])
            if not confirm_url:

                continue
            if not d['ecosystems']:
                continue


            if  eco_list not in d['ecosystems']:
                continue
            d['main_url'] = confirm_url
            d["record_name"] = d['name']
            d["stage_id"] = d['id']

            d["origin"] = 'staging'
            new_data.append(d)
            # print(json.dumps(d, indent=2))

        return new_data


    def single_select_record(self,stage_id):
        data = self.curr.execute(
            f""" select main_url, image  from start_set where done=True and 
                    found=True and needs_patch =True  and stage_id='{stage_id}'""").fetchall()
        if not data:
            return None
        data = data[0]
        if data[1] =='n/a':
            return {'url':data[0],'logo':''}
        else:
            return {'url': data[0], 'logo': data[1]}

