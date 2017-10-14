import plotly.graph_objs as go
import pyspark.sql.functions as F


def funds_bar_chart(DF, fundsInvested):
    '''
    Create a horizontal bar chart based on funds invested and analyzedDF.
    '''
    # Compute categories
    principalOutStanding, badDebtFunds = (DF.filter(F.col('completed') == False)
                                          .groupBy()
                                          .agg(F.round(F.sum('principalOutstanding'), 2).alias('principalOutstanding'), 
                                               F.sum('badDebtFundsReceived').alias('badDebtFunds'))
                                          .first())
    fundsAvailable = fundsInvested - principalOutStanding - badDebtFunds
    
    # Create dictionary
    funds = {
        'Principal<br>Outstanding': (principalOutStanding, '#9165AE'),
        'Bad Debt<br>Funds': (badDebtFunds, '#FF8000'),
        'Funds<br>Available': (fundsAvailable, '#00BBFF')
    }
    
    # Create traces
    traces = []
    
    for fundName in ['Funds<br>Available', 'Bad Debt<br>Funds', 'Principal<br>Outstanding']:
        fund, colour = funds[fundName]
        
        traces.append(go.Bar(
            x = [fund],
            marker = {'color': colour},
            hoverinfo = 'text',
            text = ['' if fund <= 0. else '${:,.2f}<br>{}'.format(fund, fundName)]
        ))
        
    data = go.Data(traces)
    
    layout = go.Layout(
        height = 100,
        xaxis = {
            'showline': False,
            'showgrid': False,
            'showticklabels': False,
            'zeroline': False
        },
        yaxis = {
            'showline': False,
            'showgrid': False,
            'showticklabels': False,
        },
        barmode = 'stack',
        showlegend = False,
        margin = {'t': 25, 'b': 25}
    )

    return {
        'data': data,
        'layout': layout
    }


def notes_distribution_bar_chart(DF):
    '''
    Plot a bar chart showing the fund distribution in each note grade.
    '''
    grades = ['A+', 'A', 'B+', 'B', 'C+', 'C', 'D+', 'D', 'E+', 'E']
    fundsDict = (DF
                 .groupBy('grade')
                 .agg(F.sum('principal'))
                 .rdd
                 .collectAsMap())
    
    trace = go.Bar(
        x = grades,
        y = [fundsDict[grade] if grade in fundsDict.keys() else 0 for grade in grades],
        marker = {'color': '#9165AE'},
    )
    
    data = go.Data([trace])
    
    layout = go.Layout(
        yaxis = {'tickprefix': '$'},
        margin = {'t': 0}
    )

    return {
        'data': data,
        'layout': layout
    }


def progress_bar(DF, grade=None):
    '''
    Creates a custom progress bar based on analyzedNotesDF, filtered on `grade`.progress_bar

    If `grade` is None, no filtering is performed.
    '''
    def _convert_to_list(colName):
        '''
        Extract the colName of notesDF as a list, with each entry rounded to 2 decimal places.
        '''
        return (DF
                .select(colName)
                .rdd
                .map(lambda x: x[0] if colName == 'company' or colName == 'monthsToBreakeven' else round(x[0], 2))
                .collect())
    
    def _create_trace(colName):
        '''
        Create a trace based on colName.
        '''
        colourDict = {
            'feesPaid': '#585858', 
            'nextFeesPayment': '#848484', 
            'feesOutstanding': '#BDBDBD', 
            'principalReceived': '#512361',
            'nextPrincipalPayment': '#9165AE',
            'principalOutstanding': '#C2B2C8',
            'badDebtFundsReceived': '#FF8000',
            'nextBadDebtFundsPayment': '#FAAC58',
            'badDebtFundsOutstanding': '#F5D0A9',
            'profitsReceived': '#72C02C', 
            'nextProfitsPayment': '#a2de6e',
            'profitsOutstanding': '#d7f1c1'
        }
        
        names = {
            'feesPaid': 'Fees Paid', 
            'nextFeesPayment': 'Next Fee Payment', 
            'feesOutstanding': 'Fees Outstanding', 
            'principalReceived': 'Principal Received',
            'nextPrincipalPayment': 'Next Principal Payment',
            'principalOutstanding': 'Principal Outstanding', 
            'badDebtFundsReceived': 'Bad Debt Funds Received',
            'nextBadDebtFundsPayment': 'Next Bad Debt Funds Payment',
            'badDebtFundsOutstanding': 'Bad Debt Funds Outstanding',
            'profitsReceived': 'Profits Received', 
            'nextProfitsPayment': 'Next Profit Payment',
            'profitsOutstanding': 'Profits Outstanding'
        }
        
        groups = {
            'feesPaid': 'Fees', 
            'nextFeesPayment': 'Fees', 
            'feesOutstanding': 'Fees',  
            'principalReceived': 'Principal',
            'nextPrincipalPayment': 'Principal',
            'principalOutstanding': 'Principal', 
            'badDebtFundsReceived': 'Bad Debt Funds',
            'nextBadDebtFundsPayment': 'Bad Debt Funds',
            'badDebtFundsOutstanding': 'Bad Debt Funds',
            'profitsReceived': 'Profits', 
            'nextProfitsPayment': 'Profits',
            'profitsOutstanding': 'Profits'
        }
        
        return go.Bar(
            x = _convert_to_list(colName),
            y = companiesList,
            name = names[colName],
            orientation = 'h',
            width = 0.75,
            hoverinfo = 'text',
            text = ['' if val == 0 else '${}'.format(val) for val in _convert_to_list(colName)],
            legendgroup = groups[colName],
            marker = {
                'color': colourDict[colName],
                'line': {'width': 0.}
            }
        )
    
    def _capitalise_first_letter(string):
        return ''.join([char.upper() if i == 0 else char for i, char in enumerate(string)])
    
    # Filter notes
    if grade != None:
        DF = DF.filter(F.col('grade') == grade)

    # Obtain list of companies
    companiesList = _convert_to_list('company')
    
    # Add relevant columns to notesDF
    categories = ['fees', 'principal', 'badDebtFunds', 'profits']
    
    # Create keys for traces
    _megaList = [['{}Received'.format(category) if category != 'fees' else 'feesPaid', 
                 'next{}Payment'.format((_capitalise_first_letter(category))), 
                 '{}Outstanding'.format(category)] for category in categories]
    
    keys = [item for sublist in _megaList for item in sublist]
    
    # Create traces
    traces = []
    for key in keys:
        traces.append(_create_trace(key))
    data = go.Data(traces)
    
    # Create annotations
    annotationData = (DF
                      .select('company', 'principal', 'monthsToBreakeven')
                      .collect())
    annotations = [{
            'x': row['principal'] - 2,
            'y': row['company'],
            'text': row['monthsToBreakeven'] if row['monthsToBreakeven'] > 0 else '',
            'font': {
                'family': 'Arial',
                'size': 14,
                'color': 'rgb(255, 255, 255)'},
            'showarrow': False
        } for row in annotationData]
    
    # Create layout
    layout = go.Layout(
        height = 30 * DF.count() + 280,
        xaxis = {
            'domain': [0.2, 1],
            'tickprefix': '$'
        },
        barmode = 'stack',
        legend = {
            'orientation': 'h',
            'traceorder': 'grouped',
            'x': 0.3
        },
        annotations = annotations,
        margin = {'t': 0 }
    )

    return {
        'data': data,
        'layout': layout,
        'config': {'displayModeBar': False}
    }