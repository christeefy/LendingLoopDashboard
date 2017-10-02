import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

from datetime import datetime as dt

app = dash.Dash()

app.config.suppress_callback_exceptions = True

app.layout = html.Div([
	html.H1('Lending Loop Dashboard'),
	dcc.Dropdown(
		id = 'period-selection',
		options = [
			{'label': i, 'value': i} for i in ['All Time', 'This Month', 'This Year']
		],
		value = 'All Time',
		clearable = False,
	),
	html.Br(),
	html.Div([
		html.Div([
			html.H2('Total Earnings'),
			html.Div(id='total-earnings-display', children='Test 1'),
		]),
		html.Div([
			html.H2('Average Net ROI'),
			html.Div(id='ROI-display', children='Test 2'),
		]),
		html.Div([
			html.H2('Funds Available'),
			html.Div(id='funds-available-display', children='Test 3'),
		]),
		html.Div([
			html.H2('Diversification Index'),
			html.Div(id='diversification-display', children='Test 4')
		])
	], style={'columnCount': 4}),
	html.Div([
		html.Div(id='funds-bar-chart', children=[
			html.H2('Funds')
		]),
		html.Div(id='notes-distribution', children=[
			html.H2('Grade Distribution of Notes')
		]),
		html.Div(id='progress-bar-chart', children=[
			html.H2('Progress of Note Payments')
		])
	], style={'columnCount': 3})

])

if __name__ == '__main__':
	app.run_server(debug=True)