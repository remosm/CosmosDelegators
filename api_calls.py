import requests
import json
import time


def fetch_validators():
    response = requests.get("https://api.cosmoscan.net/validators")
    return json.loads(response.text)


def fetch_delegators(operator_address, params):
    # try / except blocks as may hit rate limit.
    try:
        response = requests.get("https://api.cosmoscan.net/validator/" + str(operator_address) + "/delegators", params=params)
    except Exception as e:
        print(e)
        time.sleep(65)
        response = requests.get("https://api.cosmoscan.net/validator/" + str(operator_address) + "/delegators", params=params)
    return json.loads(response.text)


def fetch_delegations(flow_direction, params):
    response = requests.get("https://api.cosmoscan.net/"+flow_direction+"/volume/agg", params=params)
    return json.loads(response.text)
