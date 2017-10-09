import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html

# Import project modules
import processdata
import metrics
import dataviz

import json
from datetime import datetime as dt

# Initialise spark context and return processed data as a PySpark DataFrame
spark = processdata.init_spark()
notesDF, analyzedDF = processdata.process_data(spark)

# Configure Dash
app = dash.Dash(__name__)
app.config.suppress_callback_exceptions = True

app.layout = html.Div([
	html.H1('Lending Loop Dashboard'),
	html.Div(
		id = 'Input Components',
		children = [
		html.Div([
			html.H3('Total Funds'),
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
			html.H3('Timeframe'),
			dcc.Dropdown(
			id = 'period-selection',
			options = [
				{'label': i, 'value': i} for i in ['All Time', 'This Month', 'This Year']
			],
			value = 'All Time',
			clearable = False,
		)]),
	], style={'columnCount': 2}),
	html.Br(),
	html.Div([
		html.Div([
			html.H2('Total Earnings'),
			html.Div(
				id = 'total-earnings-display', 
				children = metrics.calc_total_earnings(analyzedDF)
				)
		]),
		html.Div([
			html.H2('Average Net ROI'),
			html.Div(
				id  = 'ROI-display', 
				children = metrics.calc_ROI(analyzedDF)
				)
		]),
		html.Div([
			html.H2('Funds Available'),
			html.Div(id='funds-available-display')
		]),
		html.Div([
			html.H2('Diversification Index'),
			html.Div(
				id = 'diversification-display', 
				children = metrics.calc_diversification(notesDF))
		])
	], style={'columnCount': 4}),
	html.Div([
		html.Div(children=[
			html.H2('Funds'),
			dcc.Graph(id='funds-distribution')
		]),
		html.Div(children=[
			html.H2('Grade Distribution of Notes'),
			dcc.Graph(
			id = 'notes-distribution',
			figure = dataviz.notes_distribution_bar_chart(notesDF)
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
				id = 'progress-bar'
			)
		])
	], style={
		'height': 1000,
	})
])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})


@app.callback(
	Output('progress-bar', 'figure'),
	[Input('notes-distribution', 'clickData')])
def update_progress_bar(clickData):
	'''
	Update graphs based on clicks on Notes Distribution
	'''
	if clickData == None:
		return dataviz.progress_bar(analyzedDF)
	return dataviz.progress_bar(analyzedDF, clickData['points'][0]['x'])

@app.callback(
	Output('notes-distribution', 'clickData'),
	[Input('reset-grade', 'n_clicks')])
def reset_grade(n_clicks):
	'''
	Reset the progress_bar visualization to view all notes, by resetting `clickData` to None
	'''
	return None

@app.callback(
	Output('funds-available-display', 'children'),
	[Input('submit-total-funds-button', 'n_clicks')],
	[State('total-funds-input', 'value')])
def update_available_funds_metric(n_clicks, totalFunds):
	'''
	Update the display value of 'Available Funds' when the 'Total Funds' is updated.
	'''
	if totalFunds == '':
		totalFunds = 0.

	return metrics.calc_funds_remaining(notesDF, float(totalFunds))

@app.callback(
	Output('funds-distribution', 'figure'),
	[Input('submit-total-funds-button', 'n_clicks')],
	[State('total-funds-input', 'value')])
def update_funds_distribution_bar_chart(n_clicks, totalFunds):
	'''
	Update the Funds Distribution Bar Chart when the 'Total Funds' is updated.
	'''
	if totalFunds == '':
		totalFunds = 0.

	return dataviz.funds_bar_chart(analyzedDF, float(totalFunds))




if __name__ == '__main__':
	app.run_server(debug=True)