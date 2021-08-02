import requests
import json
import os
import logging
import time
import sys


# call restful api
def get_request(**kwargs):
    url = kwargs["url"]
    local_path = kwargs["local_path"] #  /usr/local/airflow/temp
    headers = {'Authorization': api_key}
    stamp = 0
    offset = None
    
    try:
        logging.info(f"Calling RESTFUL endpoint : {url}")
        r = requests.get(url, headers=headers)
        assert r.status_code == 200, "Not 200..."
        raw = r.json()
        data = raw["records"]
        logging.info("The count of the data {}".format(len(data)))
        file_name = f"output_path_{stamp}.jsonl"
        save_file_path = os.path.join(local_path, file_name)
        with open(save_file_path, 'w') as outfile:
            for entry in data:
                f_entry = {k.lower(): v for k, v in entry["fields"].items()}
                if 'metadata' in f_entry:
                    f_entry['metadata'] = json.loads(f_entry['metadata'])
                json.dump(f_entry, outfile)
                outfile.write('\n')
        stamp += 1
    except:
        logging.error("Response is not 200 please check api serive")
        sys.exit(1)

    time.sleep(1) # avoid the limitation

    try:
        offset = raw["offset"] # for the next page
    except:
        logging.info("No more next page")
    
    while True:
        try:
            cond_url = url + "offset={}".format(offset)
            logging.info(f"Calling RESTFUL endpoint : {cond_url}")
            r = requests.get(cond_url.format(offset), headers=headers)
            assert r.status_code == 200, "Not 200..."
            raw = r.json()
            data = raw["records"]
            logging.info("The count of the data {}".format(len(data)))
            file_name = f"output_path_{stamp}.jsonl"
            save_file_path = os.path.join(local_path, file_name)
            with open(save_file_path, 'w') as outfile:
                for entry in data:
                    f_entry = {k.lower(): v for k, v in entry["fields"].items()}
                    json.dump(f_entry, outfile)
                    outfile.write('\n')
            stamp += 1
            time.sleep(1) # avoid the limitation
            try:
                offset = raw["offset"] # for the next page
            except:
                logging.info("No more next page")
                break
        except:
            logging.error("Response is not 200 please check api serive")
            sys.exit(1)


def clean_up():
    for file in os.listdir("./temp"):
        local_file = os.path.join(os.getcwd(), "temp", file)
        print(f"file {local_file} cleaning up...")
        os.remove(local_file)

    
if __name__ == "__main__":
    web_para = {
        "url" : "https://api.airtable.com/v0/appWzDISwYl2XFcEz/tblOc1PAd1ohZJ7lX?",
        "local_path": "/Users/kenhung/Desktop/Luko_casestudy/temp_web"
    }
    app_para = {
        "url": "https://api.airtable.com/v0/appWzDISwYl2XFcEz/tblszfEdmGU3mQfyR?",
        "local_path": "/Users/kenhung/Desktop/Luko_casestudy/temp_app"
    }
    get_request(**web_para)


