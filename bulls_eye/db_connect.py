import sys
import sqlalchemy
import os
import re
import json
from collections import  Counter
from sqlalchemy.orm import Session
from google.cloud import secretmanager
import os
from supabase import create_client, Client

#'https://ptlkltxpgvtdiybztper.supabase.co/rest/v1/staged_assets?

'''

export SUPABASE_URL="https://ptlkltxpgvtdiybztper.supabase.co"
export SUPABASE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB0bGtsdHhwZ3Z0ZGl5Ynp0cGVyIiwicm9sZSI6ImFub24iLCJpYXQiOjE2ODg2NTIxODUsImV4cCI6MjAwNDIyODE4NX0.0fWQqYKtpWPHt2Yuj77SicvkfZNJCwemqsLw8tlexcU"

curl 'https://ptlkltxpgvtdiybztper.supabase.co/rest/v1/staged_assets?select=id' \
-H "apikey: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB0bGtsdHhwZ3Z0ZGl5Ynp0cGVyIiwicm9sZSI6ImFub24iLCJpYXQiOjE2ODg2NTIxODUsImV4cCI6MjAwNDIyODE4NX0.0fWQqYKtpWPHt2Yuj77SicvkfZNJCwemqsLw8tlexcU" \
-H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB0bGtsdHhwZ3Z0ZGl5Ynp0cGVyIiwicm9sZSI6ImFub24iLCJpYXQiOjE2ODg2NTIxODUsImV4cCI6MjAwNDIyODE4NX0.0fWQqYKtpWPHt2Yuj77SicvkfZNJCwemqsLw8tlexcU"

'''


#pip install cloud-sql-python-connector==1.2.1 PyMySQL==1.0.2 google-api-core==2.11.0 google-auth==2.16.2 google-auth-oauthlib==1.0.0 google-cloud-core==2.3.2 google-cloud-secret-manager==1.0.2 google-cloud-storage==2.7.0




class CloudConnect:
    def __init__(self):
        self.version_id = '/versions/latest'
        self.project_id = 'projects/906425369069/secrets/'
        self.cloud_path = 'lib/python3.10/site-packages/working_directory/root/'
        self.production = False


        if self.production:
            full_path = self.current_path()

            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = {
            "client_id": os.getenv("client_id")[0],
            "client_secret": os.getenv("client_secret"),
            "quota_project_id": os.getenv("quota_project_id"),
            "refresh_token": os.getenv("refresh_token"),
            "type": os.getenv("type")
            }

    def current_path(self)->str:
        current = re.compile(r'^/venvs.*/b', flags=re.IGNORECASE).findall(sys.executable)
        if current:
            return current[0][:-1] + self.cloud_path


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
        if  self.production is False:
            password = "Cwodp3eateL"
            host = "localhost"
            user = "root"
            port = "3306"
            db_name = "ecomap"
        else:

            password = self.access_secret_version(f"{self.project_id}db_password{self.version_id}")
            host = self.access_secret_version(f"{self.project_id}db_host{self.version_id}")
            user = self.access_secret_version(f"{self.project_id}db_user{self.version_id}")
            db_name = "eco_db"
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
            pool_timeout=30,  # 30 seconds

            # 'pool_recycle' is the maximum number of seconds a connection can persist.
            # Connections that live longer than the specified amount of time will be
            # re-established
            pool_recycle=1800,  # 30 minutes

        )
        return pool
    def get_slack_key(self,testing=False):
        if testing:
            return self.access_secret_version(f"{self.project_id}"
                                              f"slack_testspace{self.version_id}")
        return self.access_secret_version(f"{self.project_id}slack_workspace{self.version_id}")

    def get_scraping_api_key(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"scraping_api_key{self.version_id}")

    def get_eco_endpoint(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"eco_endpoint{self.version_id}")

    def get_eco_token(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"eco_token{self.version_id}")

class BaseQuery:
    """This class constructs the building blocks for db queries used throughout the code base,
    """
    def __init__(self,testing=False):
        self.curr = CloudConnect().connect_tcp_socket().connect()
        self.config = Session(self.curr)
        self.current_records = Counter()
        self.stage_tb = 'staged_assets'
        if testing is True :
            self.stage_tb = 'dummy_image_stage'

        #get copy storage table columns
        self.get_columns = self.curr.execute(f""" DESCRIBE start_set""")
        self.keys = [i[0] for i in self.get_columns.fetchall()[1:]]
        self.query_columns = self.column_format(self.keys)
        self.url = os.environ.get("SUPABASE_URL")
        self.key = os.environ.get("SUPABASE_KEY")
        self.supabase: Client = create_client(self.url, self.key)



    def get_current_records(self):
        """see if a record already exists in the db"""
        records = self.curr.execute("""select stage_id from start_set""")
        for r in records.fetchall():
            self.current_records[f"{r[0]}"] = 1
        return 0

    def column_format(self,value:list)->str:
        '''transform list of strings into format suitable for db query'''

        # removes outer brackets and string quotations, leaving just the column names and commas
        return str(value)[1:-1].replace("'",' ')

    def save_to_copy_db(self, data):
        self.get_current_records()

        for d in data:
            stage_id = d['stage_id']



            if self.current_records[f"{stage_id}"] !=0:
                self.curr.execute(f"""update start_set
                                      set done=False, `found`=False
                                      where stage_id ={stage_id}
                                                   """)

            else:
                self.curr.execute(
                """INSERT INTO start_set (record_name, image, main_url, stage_id, needs_patch, `found`, done)
                Values( %s,%s,%s,%s,%s,%s,%s) """,
                (
                    d["record_name"],
                    d["image"],
                    d["main_url"],
                    d["stage_id"],
                    1,
                    0,
                    0
                ))
        self.config.commit()


    def find_img_data(self):

         data = self.curr.execute(f""" select {self.query_columns}  from start_set where done=False and image='n/a' """)

         data = data.fetchall()
         start_data = []
         for d in data:
            start_data.append(dict(zip(self.keys,d)))

         return start_data

    def verify_img_data(self):


        data = self.curr.execute(f""" select {self.query_columns}   from start_set where done=False  and image!='n/a' """)

        data = data.fetchall()

        start_data = []

        for d in data:
            start_data.append(dict(zip(self.keys, d)))

        return start_data

    def test_links(self):


        data = self.curr.execute(f""" select main_url  from start_set  """)

        data = data.fetchall()

        start_data = []

        for d in data:
            print(f"'{d[0]}',")

        return start_data

    def update_found(self,records, verification:bool):
        for r in records:
            stage_id = r['stage_id']
            needs_patch = 1
            image = r['image']
            #print(f"This is the image:{image}, {record}")
            if verification is True:
                # if an image has been verified, no need to re patch
                #later we may want to patch whatever column that may be apart of a trigger
                needs_patch = 0

            self.curr.execute(f"""update start_set set found=True,done=True, needs_patch=True, image= %s where stage_id='{stage_id}'""",(image))
        self.config.commit()
        return 0



    def update_none_found(self,records, verification:bool):
        for r in records:
            stage_id = r['stage_id']
            done = True
            none_found = 'n/a'
            if verification is True:
                self.curr.execute(f"""update start_set set image='{none_found}',  `found` =False, done=True where stage_id = '{stage_id}'""")
                self.config.commit()
            else:
                self.curr.execute(f"""update start_set set image='{none_found}', done=True, `found`=False, needs_patch=True where stage_id='{stage_id}'""")
        self.config.commit()


    def update_live_staging(self):
        data = self.select_processed_records()
        found_data = data[0]
        none_found_data = data[1]
        for f in found_data:
            stage_id = f['stage_id']
            image = f['image']
            self.supabase.table(self.stage_tb).update({"prod_image": image, "image_check": True}).eq("int_id", stage_id).execute()

        for n in none_found_data:
            stage_id = n['stage_id']
            image = n['image']
            self.supabase.table(self.stage_tb).update({"image_check": True}).eq("int_id", stage_id).execute()
        all_updates = none_found_data + found_data
        # make updates to done records
        self.update_checked_records(all_updates)

    def update_checked_records(self,data):
        for d in data:
            stage_id = d['stage_id']
            self.curr.execute(
                f"""update start_set set done=True, needs_patch = false where stage_id = '{stage_id}'""")
        self.config.commit()

    def select_processed_records(self):
         found_data = self.curr.execute(f""" select {self.query_columns}   from start_set where done=True and found=True and needs_patch =True and image!='n/a' """)
         none_found_data = self.curr.execute(f""" select {self.query_columns}  from start_set where done=True and found=False and needs_patch =True and image='n/a' """)
         found_data = found_data.fetchall()
         none_found_data = none_found_data.fetchall()
         found,none_found = [], []

         for f in found_data:
            found.append(dict(zip(self.keys, f)))

         for nf in none_found_data:
            none_found.append(dict(zip(self.keys, nf)))

         return found, none_found


    def select_live_staging(self):
        # selects records where image is empty and image_check is False
        #data_set = self.supabase.table("dummy_image_stage").select("stage_id", "name", "mainURL", "image").eq("image","").eq("image_check",False).execute()
        data_set = self.supabase.table(self.stage_tb).select("int_id", "Name", "mainURL").eq("image_check", "False").execute()

        for d in data_set.data:
            d['main_url'] = d["mainURL"]
            d["record_name"] = d['Name']
            d["stage_id"] = d['int_id']
            d['image'] = 'n/a'
            #print(json.dumps(d, indent=2))
        return data_set.data

    def temp_insert(self):
        with open('staging_data.json') as f:
            records = json.load(f)["Row"]
        print(len(records))
        for r in records:
            mainURL = r['mainURL']
            name=r['name']
            data = self.supabase.table("dummy_image_stage").insert({"Name": name,"mainURL":mainURL}).execute()



'''
test = BaseQuery().update_found(test_record,False)'''

'''
# python3 bulls_eye/db_connect.py
    
'''