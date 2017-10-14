import dash_core_components as dcc
import dash_html_components as html

from app import app


layout = html.Div([
    html.Div(
        id = 'input-components',
        children = [
        html.Div([
            html.H2('Total Funds'),
            dcc.Input(
                id = 'total-funds-input',
                placeholder='Enter total funds...',
                type='text',
                value=''
            ),
            html.Button(
                id = 'submit-total-funds-button',
                children = 'Submit'
            )
        ]),
        html.Br(),
        html.Div([
            html.H2('Timeframe'),
            dcc.Dropdown(
            id = 'period-selection',
            options = [
                {'label': i, 'value': i} for i in ['All Time', 'This Month', 'This Year']
            ],
            value = 'All Time',
            clearable = False,
        )]),
    ], style={
        'columnCount': 2
    }),
    html.Br(),
    html.Div([
        html.Div([
            html.H2('Total Earnings'),
            html.Div(
                id = 'total-earnings-display', 
            )
        ]),
        html.Div([
            html.H2('Current ROI'),
            html.Div(
                id  = 'current-ROI-display', 
            )
        ]),
        html.Div([
            html.H2('Net ROI'),
            html.Div(
                id = 'net-ROI-display'
            )
        ])
    ], style={
        'columnCount': 3
    }),
    html.Div([
        html.Div([
            html.H2('Funds Available'),
            html.Div(id='funds-available-display')
        ]),
        html.Div([
            html.H2('Diversification Index'),
            html.Div(
                id = 'diversification-display', 
            )
        ])
    ], style={'columnCount': 2}),
    html.Br(),
    html.Div([
        html.Div(children=[
            html.H2('Funds'),
            dcc.Graph(
                id = 'funds-distribution',
                config = {'displayModeBar': False})
        ]),
        html.Div(children=[
            html.H2('Grade Distribution of Notes'),
            dcc.Graph(
                id = 'notes-distribution',
                config = {'displayModeBar': False}
            ),
            html.Button(
                id = 'reset-grade', 
                n_clicks = 0,
                children = 'All grades'
            )
        ]),
        html.Br(),
        html.Div(children=[
            html.H2('Progress of Note Payments'),
            dcc.Graph(
                id = 'progress-bar',
                config = {'displayModeBar': False}
            )
        ])
    ], style={
        'height': 1000,
    })
])