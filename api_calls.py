import requests
import json
import time


def fetch_validators():
    response = requests.get("https://api.cosmoscan.net/validators")
    validator_overview = json.loads(response.text)
    return validator_overview


def fetch_delegators(operator_address, params):
    # try / except blocks as may hit rate limit.
    try:
        response = requests.get("https://api.cosmoscan.net/validator/" + str(operator_address) + "/delegators", params=params)
    except Exception as e:
        print(e)
        time.sleep(65)
        response = requests.get("https://api.cosmoscan.net/validator/" + str(operator_address) + "/delegators", params=params)
    return response


def fetch_undelegations(params):
    response = requests.get("https://api.cosmoscan.net//undelegations/volume/agg", params=params)
    undelegations = json.loads(response.text)
    return undelegations
