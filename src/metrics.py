import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T


def _calc_weighted_avg(DF, vals, weights):
    '''
    Returns the weightsed average of column `vals` 
    in `DF` with weightss `weights`.
    '''
    @F.udf(returnType=T.FloatType())
    def _weighted_avg(vals, weights, sumOfweightss):
        return vals * weights / sumOfweightss
    
    sumOfweightss = DF.groupBy().agg(F.sum(weights)).first()[0]
    
    return (DF.select(F.col(vals) * F.col(weights))
           .groupBy()
           .sum()
           .first()[0]) / sumOfweightss


@F.udf(returnType=T.FloatType())
def _annualize_return_rate(projEarnings, principal, cyclesTotal, cyclesRemaining=0):
    if cyclesTotal == cyclesRemaining:
        return 1.
    return (projEarnings / principal + 1)**(1. / ((cyclesTotal - cyclesRemaining) / 12.))


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


def calc_current_ROI(DF):
    _DF = (DF
              .select(_annualize_return_rate(F.col('badDebtFundsReceived') 
                                            + F.col('profitsReceived'),
                                            'principal',
                                            'cyclesTotal',
                                            'cyclesRemaining')
                      .alias('annualizedReturnRate'),
                      'principal'))
    
    result = (_calc_weighted_avg(_DF, vals='annualizedReturnRate', weights='principal') - 1) * 100
    
    return '{:.1f}%'.format(result)


def calc_net_ROI(DF):
    '''
    Calculate the projected net ROI, by accounting for 
    Lending Loop's servicing fee and the bad debt rate.
    '''
    @F.udf(returnType=T.FloatType())
    def annualize_return_rate(projEarnings, principal, cyclesTotal):
        return (projEarnings / principal + 1)**(1. / (cyclesTotal / 12))

    _DF = (DF
           .select(_annualize_return_rate(F.col('profitsReceived') 
                                         + F.col('nextProfitsPayment') 
                                         + F.col('profitsOutstanding'),
                                         'principal', 
                                         'cyclesTotal')
                   .alias('annualizedReturnRate'), 
                   'principal'))
    
    res = (_calc_weighted_avg(_DF, vals='annualizedReturnRate', weights='principal') - 1) * 100
    
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