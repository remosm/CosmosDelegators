import requests
import json
import time
import pandas as pd
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")


def fetch_validators():
    response = requests.get("https://api.cosmoscan.net/validators")
    validator_overview = json.loads(response.text)
    return validator_overview


def fetch_delegators(operator_address, params):
    try:
        response = requests.get("https://api.cosmoscan.net/validator/" + str(operator_address) + "/delegators", params=params)
    except Exception as e:
        print(e)
        time.sleep(65)
        response = requests.get("https://api.cosmoscan.net/validator/" + str(operator_address) + "/delegators", params=params)
    return response


def aggregate_delegators(operator_address, max_delegators, operator_name='chorus_one', file_n='_delegators_atom.csv'):
    delegator_frame = pd.DataFrame(columns=['delegator', 'amount', 'since', 'delta'])

    last_fetch = 0
    while len(delegator_frame) < max_delegators:
        params = {'limit': min(20, max_delegators-len(delegator_frame)), 'offset': len(delegator_frame)}

        response = fetch_delegators(operator_address, params)
        delegators = json.loads(response.text)
        delegator_frame = delegator_frame.append(delegators['items'], ignore_index=True)

        print('Validator: ', operator_name, '. Fetched: ', len(delegator_frame), '. Share fetched: ', len(delegator_frame) / max_delegators)

        if len(delegator_frame) == last_fetch:
            # Occasionally validators un-stake while the update is running.
            print('Failed to update. Returning.')
            if len(delegator_frame) / max_delegators < 0.98:
                file_n = '_incomplete_'+file_n
            break

        last_fetch = len(delegator_frame)
        time.sleep(0.3)

    timestamp = datetime.strftime(datetime.utcnow(), "%s")
    delegator_frame.to_csv('./data/'+timestamp+'_'+operator_name+file_n, index=False)


def __main__():
    validators = fetch_validators()

    validator_stats = pd.DataFrame(columns=validators[0].keys())
    for val in validators:
        validator_stats = validator_stats.append(val, ignore_index=True)

    validator_stats = validator_stats.sort_values(by=['percent_power'], ascending=False)
    validator_stats = validator_stats.reset_index(drop=True)
    validator_stats.to_csv('all_validators.csv', index=False)

    validator_stats = validator_stats[137:140]
    for index, val in validator_stats.iterrows():
        print('Fetching delegator data for..', val['title'])
        aggregate_delegators(val['operator_address'], val['delegators'], operator_name=val['title'])

__main__()
