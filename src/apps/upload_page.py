import dash_core_components as dcc
import dash_html_components as html

from app import app


layout = html.Div([
    html.H3('Upload all_payments.csv'),
    dcc.Upload(
        id = 'upload-box',
        children = html.A('Drag and drop \'all_payments.csv\' here!\n'),
        style = {
            'height': '75px',
            'lineHeight': '75px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'borderWidth': '1px',
            'textAlign': 'center'
        }
    ),
    html.Br(),
    html.Div(id='upload-status')
])