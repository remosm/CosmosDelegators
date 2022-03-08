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
