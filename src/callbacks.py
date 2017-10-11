from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html

from app import app
from apps import main_page, upload_page

import processdata
import metrics
import dataviz

import json
from datetime import datetime as dt
import io
import base64



DASHBOARD_PATHNAME = '/main'

@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')])
def update_page_content(pathname):
    '''
    Update page content based on pathname.
    '''
    if pathname == '/':
        return upload_page.layout
    elif pathname == DASHBOARD_PATHNAME:
        return main_page.layout
    else:
        return ''


@app.callback(
    Output('upload-status', 'children'),
    [Input('upload-box', 'contents')])
def analyze_dataframes(content):
    '''
    Upon receiving a valid csv file, creates PySpark instance to
    analyze dataframe. 

    The results are depicted in upload-status. A successful upload
    and analysis produces a link to '/main'.
    '''

    # Return nothing if content is blank
    if not content:
        return None

    # Obtain type of content
    contentType, contentString = content.split(',')

    if 'csv' in contentType:
        print 'CSV file confirmed.'
        # Initialise spark context and return processed data as a PySpark DataFrame 
        spark = processdata.init_spark()

        # Declare global variables
        global notesDF
        global analyzedDF

        try:
            _string = base64.b64decode(contentString).decode('utf-8')
            notesDF, analyzedDF= processdata.process_data(spark, io.StringIO(_string))
        except Exception as e:
            print 'Error analyzing data.\n{}'.format(e)
            return 'Error analyzing data.'
        print 'Analysis complete!'

        layout = html.Div(
            [html.P('Dataset analysis successful!'), 
            dcc.Link('View your dashboard.', href='/main')]
        )

        return layout

    return None


@app.callback(
    Output('total-earnings-display', 'children'),
    [Input('submit-total-funds-button', 'n_clicks')])
def update_metrics_total_earnings(n_clicks):
    return metrics.calc_total_earnings(analyzedDF)


@app.callback(
    Output('ROI-display', 'children'),
    [Input('submit-total-funds-button', 'n_clicks')])
def update_metrics_ROI(pathname):
    return metrics.calc_ROI(analyzedDF)


@app.callback(
    Output('funds-available-display', 'children'),
    [Input('submit-total-funds-button', 'n_clicks')],
    [State('total-funds-input', 'value')])
def update_metrics_available_funds(n_clicks, totalFunds):
    '''
    Update the display value of 'Available Funds' when the 'Total Funds' is updated.
    '''
    return metrics.calc_funds_remaining(analyzedDF, float(totalFunds))


@app.callback(
    Output('diversification-display', 'children'),
    [Input('submit-total-funds-button', 'n_clicks')])
def update_metrics_diversification(pathname):
    return metrics.calc_diversification(notesDF)


@app.callback(
    Output('funds-distribution', 'figure'),
    [Input('submit-total-funds-button', 'n_clicks')],
    [State('total-funds-input', 'value')])
def display_funds_distribution_bar_chart(n_clicks, totalFunds):
    '''
    Display / update the Funds Distribution Bar Chart when the 'Total Funds' is updated.
    '''
    if totalFunds == '':
        totalFunds = 0.

    return dataviz.funds_bar_chart(analyzedDF, float(totalFunds))


@app.callback(
    Output('notes-distribution', 'figure'),
    [Input('submit-total-funds-button', 'n_clicks')])
def display_notes_distributions_bar_chart(n_clicks):
    return dataviz.notes_distribution_bar_chart(notesDF)

@app.callback(
    Output('progress-bar', 'figure'),
    [Input('notes-distribution', 'clickData')])
def display_progress_bar(clickData):
    '''
    Display / update graphs based on clicks on Notes Distribution
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


