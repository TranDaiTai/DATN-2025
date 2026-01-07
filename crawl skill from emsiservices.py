# native packs
import requests
import json
# installed packs
import pandas as pd
from pandas import json_normalize


auth_endpoint = "https://auth.emsicloud.com/connect/token"  # auth endpoint

# replace 'your_client_id' with your client id from your api invite email
client_id = "5f379hywuvh7fvan"
# replace 'your_client_secret' with your client secret from your api invite email
client_secret = "hfCkXQEy"
scope = "emsi_open"  # ok to leave as is, this is the scope we will used

# set credentials and scope
payload = f"client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials&scope={scope}"
# headers for the response
headers = {'content-type': 'application/x-www-form-urlencoded'}
access_token = json.loads((requests.request("POST", auth_endpoint, data=payload, headers=headers)).text)[
    'access_token']  # grabs request's text and loads as JSON, then pulls the access token from that


def fetch_skills_list(save_path="skills.json"):
    all_skills_endpoint = "https://emsiservices.com/skills/versions/latest/skills"
    auth = f"Bearer {access_token}"
    headers = {"Authorization": auth}

    response = requests.get(all_skills_endpoint, headers=headers)
    response.raise_for_status()

    data = response.json()["data"]

    # 💾 Lưu file JSON
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        print("lưu thành công") 

    return data

fetch_skills_list() ; 