import os
import pandas as pd
import numpy as np
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")


def list_validators_fetched(path='./data/', chain='atom'):
    validators_csv = [i for i in os.listdir(path) if chain in i]
    validators = [i[:i.find('_')] for i in validators_csv]
    validator_dict = dict(zip(validators, validators_csv))
    return validator_dict


def load_data(path='./data/', chain='atom'):
    validator_dict = list_validators_fetched(path, chain)

    val_dfs = []
    max_delegators = 0
    for val in validator_dict.keys():
        val_df = pd.read_csv(path+validator_dict[val]).add_prefix(val+'_')
        max_delegators = max(max_delegators, val_df.shape[0])
        val_dfs.append(val_df)

    validators_df = pd.DataFrame(np.zeros((max_delegators, 1)))
    for val_df in val_dfs:
        validators_df = validators_df.join(val_df)
    validators_df = validators_df.iloc[:, 1:]
    return validators_df


def staked_with_others(validator, delegators, validators_df):
    staked_df = pd.DataFrame(np.zeros((len(delegators), len(validators_df.columns))), columns=validators_df.columns)
    staked_df[validator+'_delegator'] = delegators

    for col in validators_df.columns:
        if '_delegator' in col:
            for de in delegators:
                if de in validators_df[col].values:
                    peer = col[:col.find('_')]
                    staked_df.loc[staked_df[validator+'_delegator'] == de, peer+'_delegator'] = de
                    staked_df.loc[staked_df[validator + '_delegator'] == de, peer+'_amount'] = float(validators_df.loc[validators_df[peer+'_delegator'] == de, peer+'_amount'].values[0])
                    staked_df.loc[staked_df[validator + '_delegator'] == de, peer + '_since'] = validators_df.loc[validators_df[peer + '_delegator'] == de, peer + '_since'].values[0]
                    staked_df.loc[staked_df[validator + '_delegator'] == de, peer + '_delta'] = validators_df.loc[validators_df[peer + '_delegator'] == de, peer + '_delta'].values[0]
    return staked_df


def fetch_delegator_range(validator, validators_df, simple_range=None):
    delegators_range = {}
    validator_df = validators_df[[col for col in validators_df.columns if validator in col]]
    validator_df = validator_df.dropna()

    if simple_range is not None:
        delegators_range[str(simple_range[0]) + '_' + str(simple_range[1])] = list(validator_df[(validator_df[
                                                                                     validator + '_amount'] <= simple_range[1]) & (
                                                                                            validator_df[
                                                                                                validator + '_amount'] > simple_range[0])][
                                                                       validator + '_delegator'].unique())
        return delegators_range

    range = (0.01, 10000000)
    max_range = range[0]
    min_range = 0.0

    while max_range <= range[1]:
        delegators_range[str(min_range)+'_'+str(max_range)] = list(validator_df[(validator_df[validator+'_amount'] <= max_range) & (validator_df[validator+'_amount'] > min_range)][validator+'_delegator'].unique())#.values)
        min_range = max_range
        max_range *= 10
    return delegators_range


def staked_by_validator(validator):
    validators_df = load_data()
    validator_df = validators_df[[col for col in validators_df.columns if validator in col]]

    range = (0.01, 10000000)
    max_range = range[0]
    min_range = 0.0

    dash_dict = []
    while max_range <= range[1]:
        validator_df_snap = validator_df.loc[(validator_df[validator + '_amount'] <= max_range) & (
                    validator_df[validator + '_amount'] > min_range)].reset_index(drop=True)
        min_range = max_range
        max_range *= 10

        sum_staked = round(np.sum(validator_df_snap[validator + '_amount']), 2)
        num_addresses = len(validator_df_snap)
        dash_dict.append({'ATOM Range': str(min_range) + ' to ' + str(max_range), '# Addresses': num_addresses,
                          'Total ATOM': sum_staked, 'Stake share (%)': sum_staked, 'Address share (%)': num_addresses})

    total_staked = round(sum(item['Total ATOM'] for item in dash_dict), 2)
    total_addresses = sum(item['# Addresses'] for item in dash_dict)
    dash_dict.append({'ATOM Range': 'TOTAL', '# Addresses': total_addresses, 'Total ATOM': total_staked,
                      'Stake share (%)': total_staked, 'Address share (%)': total_addresses})

    cumulative_total = 0
    cumulative_delegators = 0
    for d in dash_dict:
        d.update((k, round((v / total_staked) * 100, 2)) for k, v in d.items() if k == "Stake share (%)")
        d.update((k, round((v / total_addresses) * 100, 2)) for k, v in d.items() if k == "Address share (%)")
        if d['ATOM Range'] == 'TOTAL':
            continue
        cumulative_total += d['Total ATOM']
        cumulative_delegators += d['# Addresses']
        d['Cumulative Addresses'] = cumulative_delegators
        d['Cumulative Total'] = cumulative_total
    return dash_dict


def staked_by_validators(drop_total=False):
    validators = list_validators_fetched()

    df = pd.DataFrame(columns=['Validator', 'ATOM Range', 'Cumulative Total', 'Cumulative Addresses'])
    for val in list(validators.keys()):
        d = pd.DataFrame.from_dict(staked_by_validator(val))
        d.insert(0, 'Validator', val)
        df = df.append(d, ignore_index=True)

    if drop_total:
        df = df[df["ATOM Range"].str.contains("TOTAL")==False]
    return df


def crossdelegations(minimum, maximum, validator):
    validators_df = load_data()
    validators = list_validators_fetched()

    delegators_range = fetch_delegator_range(validator, validators_df, simple_range=[minimum, maximum])

    for key in delegators_range.keys():
        staked_df = staked_with_others(validator, delegators_range[key], validators_df)

        dash_dict = []
        for val in validators:
            unique_delegators = len(staked_df.loc[staked_df[val + '_delegator'] != 0])
            total_staked = int(np.sum(staked_df[val + '_amount']))
            average_staked = int(total_staked / max(unique_delegators, 1))
            average_stake_since = int(np.sum(staked_df[val + '_since'])) / max(unique_delegators, 1)

            dash_dict.append({'Validator': val, 'Delegators': unique_delegators, 'Total staked': total_staked,
                              'Average staked': average_staked,
                              'Average date staked': datetime.fromtimestamp(average_stake_since).strftime('%Y-%m-%d')})

        dash_dict = sorted(dash_dict, key=lambda d: d['Total staked'], reverse=True)
        val_index = next((index for (index, d) in enumerate(dash_dict) if d["Validator"] == validator), None)
        dash_dict.insert(0, dash_dict.pop(val_index))
        return dash_dict
