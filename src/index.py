import dash_core_components as dcc
import dash_html_components as html

# Import layout modules
from app import app

# Import functional modules
import callbacks

app.layout = html.Div([
	html.H1('Lending Loop Dashboard'),
	dcc.Location(
		id = 'url',
		refresh = False
	),
	html.Div(id='page-content')
])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

# TODO: Prevent results from being lost when reloaded


if __name__ == '__main__':
	app.run_server(debug=True)