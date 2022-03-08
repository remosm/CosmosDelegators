import os
import pandas as pd
import numpy as np


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

#validators_df = load_data()
#print(fetch_delegator_range('Chorus One', validators_df, simple_range=[100, 110]))