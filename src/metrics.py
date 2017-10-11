import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

def calc_total_earnings(DF):
    '''
    Calculate total earnings based on analyzedNotesDF since a certain date. 
    '''
    # TODO: Make result dependent on dates
    # if since != None:
        # assert isinstance(since, datetime.datetime)
        # DF = DF.filter()
    res = (DF
            .select((F.col('badDebtFundsReceived') + F.col('profitsReceived')).alias('earnings'))
            .groupBy()
            .agg(F.sum('earnings')).first()[0])

    return '${:,.2f}'.format(res)


def calc_ROI(DF):
    result = (DF
            .select((F.col('badDebtFundsReceived') + F.col('profitsReceived')).alias('earnings'),
                    'principal')
            .groupBy()
            .agg(F.sum('earnings').alias('earnings'), F.sum('principal').alias('principal')).first())

    res = result['earnings'] / result['principal'] * 100
    
    return '{:.1f}%'.format(res)


def calc_diversification(DF):
    '''
    Diversification is calculated as per Lending Loop's dashboard: maximum investment divided by total investment.
    '''
    result = (DF
              .groupBy()
              .agg(F.sum('principal').alias('totalPrincipal'), 
                   F.max('principal').alias('maxPrincipal'))
              .first())

    res = result['maxPrincipal'] / result['totalPrincipal'] * 100

    return '{:.1f}%'.format(res)


def calc_funds_remaining(DF, totalFunds):
    res = totalFunds - DF.groupBy().agg(F.sum('principalOutstanding')).first()[0]
    return '${:,.2f}'.format(res)