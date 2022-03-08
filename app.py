import pandas as pd
import numpy as np
from datetime import datetime
import dash_daq as daq
from dash import Dash, dash_table, html, dcc
from dash.dependencies import Input, Output

import data_processing

pd.options.plotting.backend = 'plotly'

validator_list = list(data_processing.list_validators_fetched().keys())

'''
#### Dash and Markdown

Dash supports [Markdown](http://commonmark.org/help).

Markdown is a simple way to write and format text.
It includes a syntax for things like **bold text** and *italics*,
[links](http://commonmark.org/help), inline `code` snippets, lists,
quotes, and more.
'''

app = Dash(__name__)
server = app.server
app.title = 'Delegator Analysis Playground'
app.layout = html.Div([
    dcc.Markdown('''
    ### Delegator Insights Playground  
    *Work in progress - a notepad. Not exhaustive or finalized. Yet undiscovered biases possible.*
    * A table showing relative cross-validator staking allocations, based on a validator.  
    * A table showing the distribution of stakes across stake accounts, for a validator of choice. 
    #### How do delegators of a given size, for a validator, stake with other validators?
    **Instructions:**
    * Select a validator from the dropdown. The script will fetch all delegator addresses staked with this validator.
    * Define a range by selecting a minimum and maximum. The script will filter delegator addresses by staked amount, accordingly.
    * &rarr; The script will fetch & describe the delegations to other delegators for these addresses.
      
    **Sample questions** 
    * What relative share of their staking allocations do delegators trust us with, on average?
    * When to delegators start staking with a given validator on average (danger: survivorship bias. Informative, but wouldn't publish.)?
    * &rarr; What distinguishes Chorus? We could have a relatively higher average stake, for a range. We could also have relatively more delegators, with a lower stake on average (*'the people's validator'*).
    * What share of their portfolio do delegators typically stake / entrust to specific validators? (Not covered yet; would need to fetch by-account data.)
    * How many validators do delegators stake with on average? (Not covered yet; would need to run on a by-account basis)
    
    
    *The table may take up to a minute to load, depending on the range covered. Usually, it will be much quicker. Check the tab icon to track status.*
    '''),
    dcc.Dropdown(validator_list, 'Chorus One', id='validator_dropdown_comparison'),
    daq.NumericInput(
        id='my-numeric-input-1',
        label='Minimum ATOM staked',
        min=0,
        max=10000000,
        value=500
    ),
   daq.NumericInput(
        id='my-numeric-input-2',
        label='Maximum ATOM staked',
        min=0,
        max=10000000,
        value=1000
    ),
    #html.Div(id='numeric-input-output-1'),
    dash_table.DataTable(
        id='table',
        data=[]
    ),
    dcc.Markdown('''
        #### What is the relative distribution of stake sizes vs. total staked?
        **Instructions**
        * Select a validator from the dropdown. The script will display statistics for this validator.

        *The table should update within max. 5 seconds.*
        '''),
    dcc.Dropdown(validator_list, 'Chorus One', id='validator_dropdown'),
     dash_table.DataTable(
        id='staked_by_table',
        data=[]
    ),
])

@app.callback(
    Output('table', 'data'),
    [Input('my-numeric-input-1', 'value'),
    Input('my-numeric-input-2', 'value'),
    Input('validator_dropdown_comparison', 'value'),]
)
def crossdelegations_table(minimum, maximum, validator):
    print(minimum, maximum)
    validators_df = data_processing.load_data()
    validators = data_processing.list_validators_fetched()

    delegators_range = data_processing.fetch_delegator_range(validator, validators_df, simple_range=[minimum, maximum])

    for key in delegators_range.keys():
        staked_df = data_processing.staked_with_others(validator,delegators_range[key], validators_df)

        dash_dict = []
        for val in validators:
            unique_delegators = len(staked_df.loc[staked_df[val+'_delegator'] != 0])
            total_staked = int(np.sum(staked_df[val+'_amount']))
            average_staked = int(total_staked / max(unique_delegators, 1))
            average_stake_since  = int(np.sum(staked_df[val+'_since'])) / max(unique_delegators, 1)

            dash_dict.append({'Validator': val, 'Delegators': unique_delegators, 'Total staked': total_staked,
                              'Average staked': average_staked, 'Average date staked': datetime.fromtimestamp(average_stake_since).strftime('%Y-%m-%d')})

        print('FINISHED')
        dash_dict = sorted(dash_dict, key=lambda d: d['Total staked'], reverse=True)
        val_index = next((index for (index, d) in enumerate(dash_dict) if d["Validator"] == validator), None)
        dash_dict.insert(0, dash_dict.pop(val_index))
        return dash_dict


@app.callback(
    Output('staked_by_table', 'data'),
    Input('validator_dropdown', 'value'),
)
def staked_by_validator_table(validator):
    validators_df = data_processing.load_data()
    validator_df = validators_df[[col for col in validators_df.columns if validator in col]]

    range = (0.01, 10000000)
    max_range = range[0]
    min_range = 0.0

    dash_dict = []
    while max_range <= range[1]:
        validator_df_snap = validator_df.loc[(validator_df[validator+'_amount'] <= max_range) & (validator_df[validator+'_amount'] > min_range)].reset_index(drop=True)
        min_range = max_range
        max_range *= 10

        sum_staked = round(np.sum(validator_df_snap[validator+'_amount']), 2)
        num_addresses = len(validator_df_snap)
        dash_dict.append({'ATOM Range': str(min_range) + ' to ' + str(max_range), '# Addresses': num_addresses, 'Total ATOM': sum_staked, 'Stake share (%)': sum_staked, 'Address share (%)': num_addresses})

    total_staked = round(sum(item['Total ATOM'] for item in dash_dict), 2)
    total_addresses = sum(item['# Addresses'] for item in dash_dict)
    dash_dict.append({'ATOM Range': 'TOTAL', '# Addresses': total_addresses, 'Total ATOM': total_staked, 'Stake share (%)': total_staked, 'Address share (%)': total_addresses})

    for d in dash_dict:
        d.update((k, round((v/total_staked)*100, 2)) for k, v in d.items() if k == "Stake share (%)")
        d.update((k, round((v / total_addresses) * 100, 2)) for k, v in d.items() if k == "Address share (%)")
    #dash_dict = sorted(dash_dict, key=lambda d: d['Stake share (%)'], reverse=True)
    return dash_dict

#staked_by_validator_table('Chorus One')

if __name__ == '__main__':
    app.run_server(debug=True)
