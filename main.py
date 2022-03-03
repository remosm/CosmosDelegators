import time
import pandas as pd
import warnings
from datetime import datetime

import api_calls

warnings.filterwarnings("ignore")


def aggregate_delegators(operator_address, max_delegators, operator_name='chorus_one', file_n='_delegators_atom.csv'):
    delegator_df = pd.DataFrame(columns=['delegator', 'amount', 'since', 'delta'])

    last_fetch = 0
    while len(delegator_df) < max_delegators:
        params = {'limit': min(20, max_delegators-len(delegator_df)), 'offset': len(delegator_df)}

        delegators = api_calls.fetch_delegators(operator_address, params)
        delegator_df = delegator_df.append(delegators['items'], ignore_index=True)

        print('Validator: ', operator_name, '. Fetched: ', len(delegator_df), '. Share fetched: ', len(delegator_df) / max_delegators)

        if len(delegator_df) == last_fetch:
            # Occasionally validators un-stake while the update is running.
            print('Failed to update. Returning.')
            if len(delegator_df) / max_delegators < 0.98:
                file_n = '_incomplete_'+file_n
            break

        last_fetch = len(delegator_df)
        time.sleep(0.3)

    timestamp = datetime.strftime(datetime.utcnow(), "%s")
    delegator_df.to_csv('./data/'+timestamp+'_'+operator_name+file_n, index=False)
    return


def aggregate_delegations(flow_direction, timeframe='hour'):
    csv_name = 'aggregate_'+flow_direction+'.csv'
    try:
        delegation_df = pd.read_csv('./data/delegations/'+csv_name)
        fetch_from = int(delegation_df['time'].iloc[-1])
    except FileNotFoundError:
        print(csv_name+' not found.')
        delegation_df = pd.DataFrame(columns=['time', 'value'])
        fetch_from = 0

    params = {'by': timeframe, 'from': fetch_from}
    delegations = api_calls.fetch_delegations(params)

    lines_added = 0
    for delegation in delegations:
        delegation_df = delegation_df.append(delegation, ignore_index=True)
        lines_added += 1

    print('Added '+str(lines_added)+' periods to '+csv_name)
    delegation_df.to_csv('./data/delegations/'+csv_name, index=False)
    return


def __main__():
    validators = api_calls.fetch_validators()

    validator_stats = pd.DataFrame(columns=validators[0].keys())
    for val in validators:
        validator_stats = validator_stats.append(val, ignore_index=True)

    validator_stats = validator_stats.sort_values(by=['percent_power'], ascending=False)
    validator_stats = validator_stats.reset_index(drop=True)
    validator_stats.to_csv('./data/all_validators.csv', index=False)

    validator_stats = validator_stats[:20]
    for index, val in validator_stats.iterrows():
        print('Fetching delegator data for..', val['title'])
        aggregate_delegators(val['operator_address'], val['delegators'], operator_name=val['title'])

    print('Updating aggregate delegations and undelegations..')
    aggregate_delegations('delegations', timeframe='hour')
    aggregate_delegations('undelegations', timeframe='hour')
    return


__main__()
