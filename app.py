import dash_daq as daq
from dash import Dash, dash_table, html, dcc
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go


import data_processing

validator_list = list(data_processing.list_validators_fetched().keys())

app = Dash(__name__)
server = app.server
app.title = 'Delegator Analysis Playground'

app.layout = html.Div([
    dcc.Markdown('''
    ### Delegator Insights Playground  
    *Work in progress - a notepad. Not exhaustive or finalized. Yet undiscovered biases possible.*
    * A table showing relative cross-validator staking allocations, based on a validator.  
    * A table showing the distribution of stakes across stake accounts, for a validator of choice. 
    * A reactive line graph showing the distribution of stakes across stake accounts, for all validators.
    * A reactive histogram showing the distribution of delegator addresses across stake accounts, for all validators.
    * A scatter plot showing # delegations and total stake for all validators. Optionally: a clustering algorithm superimposed.
    '''),
    dcc.Markdown('''
        #### Validator clusters by stake and # of addresses. 
        **Instructions**
        * Select validators to display on the graph's sidebar.  
        * Press the button to superimpose a basic clustering algorithm.

        *The Graph may take up to 1m to load.*
        '''),
    html.Button('K-means clustering', id='cluster_button', n_clicks=0),
    dcc.Graph(id='aggregate_staked_delegators_fig'),
    dcc.Markdown('''
        #### Relative stake distribution across validators.  
        **Instructions**
        * Select validators to display on the graph's sidebar.  
        
        *The Graph may take up to 30s to load.*
        '''),
    dcc.Graph(id='staked_by_figure'),
    dcc.Markdown('''
        #### Relative address distribution across validators. 
        **Instructions**
        * Select validators to display on the graph's sidebar.  

        *The Graph may take up to 30s to load.*
        '''),
    dcc.Graph(id='staked_by_address'),
    dcc.Markdown('''
    #### How do delegators of a given size, for a validator, stake with other validators?
    **Instructions:**
    * Select a validator from the dropdown. The script will fetch all delegator addresses staked with this validator.
    * Define a range by selecting a minimum and maximum. The script will filter delegator addresses by staked amount, accordingly.
    * &rarr; The script will fetch & describe the delegations to other delegators for these addresses.

    *The table may take up to a minute to load, depending on the range covered. Usually, it will be much quicker. Check the tab icon to track status.*  
    '''),
    dcc.Dropdown(validator_list, 'Chorus One', id='validator_dropdown_comparison'),
    daq.NumericInput(
        id='min_atom',
        label='Minimum ATOM staked',
        min=0,
        max=10000000,
        value=500
    ),
    daq.NumericInput(
        id='max_atom',
        label='Maximum ATOM staked',
        min=0,
        max=10000000,
        value=1000
    ),
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
    [Input('min_atom', 'value'),
    Input('max_atom', 'value'),
    Input('validator_dropdown_comparison', 'value'),]
)
def crossdelegations_table(minimum, maximum, validator):
    return data_processing.crossdelegations(minimum, maximum, validator)


@app.callback(
    Output('staked_by_table', 'data'),
    Input('validator_dropdown', 'value'),
)
def staked_by_validator_table(validator):
    return data_processing.staked_by_validator(validator)


@app.callback(
    Output('staked_by_figure', 'figure'),
    Input('validator_dropdown', 'value'),
)
def staked_by_validators_figure(dummy_input):
    df = data_processing.staked_by_validators(drop_total=True, load_csv=True)
    fig = px.line(df, x='ATOM Range', y='Cumulative Total', color='Validator')
    return fig


@app.callback(
    Output('staked_by_address', 'figure'),
    Input('validator_dropdown', 'value'),
)
def staked_by_validators_figure(dummy_input):
    df = data_processing.staked_by_validators(drop_total=True, load_csv=True)
    fig = px.histogram(df, x="ATOM Range", y="# Addresses", color="Validator", text_auto=True)#
    return fig

@app.callback(
    Output('aggregate_staked_delegators_fig', 'figure'),
    Input('cluster_button', 'n_clicks'),
)
def staked_by_validators_figure(k_means):
    color = 'Validator'
    if k_means %2 != 0:
        color = 'Labels'
    df = data_processing.aggregate_staked_by_validators(load_csv=True)
    fig = px.scatter(x=df['Total Addresses'], y=df['Total Staked'], color=df[color], text=df['Validator'])
    fig.update_traces(textposition="top center", marker=dict(size=18))
    return fig



if __name__ == '__main__':
    app.run_server(debug=True)

"""
    **Sample questions** 
    * What relative share of their staking allocations do delegators trust us with, on average?
    * When to delegators start staking with a given validator on average (danger: survivorship bias. Informative, but wouldn't publish.)?
    * &rarr; What distinguishes Chorus? We could have a relatively higher average stake, for a range. We could also have relatively more delegators, with a lower stake on average (*'the people's validator'*).
    * What share of their portfolio do delegators typically stake / entrust to specific validators? (Not covered yet; would need to fetch by-account data.)
    * How many validators do delegators stake with on average? (Not covered yet; would need to run on a by-account basis)
"""