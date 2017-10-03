import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T


def obtain_notes(DF):
    '''
    Return a Spark DataFrame containing summaries of notes in each row.
    '''
    @F.udf(returnType=T.DoubleType())
    def calculate_principal(unitPay, interestRate, totalPayCycles):
        '''
        Calculate the principal invested based on the 
        unitPay, interestRate and totalPayCycles.
        '''
        interestRate /= 100 * 12

        return unitPay / interestRate * (1 - 1. / (1 + interestRate)**totalPayCycles)

    @F.udf(returnType=T.FloatType())
    def bad_debt_funds(principal, grade, cyclesTotal):
        rate = interestRatesBroadcast.value[grade] / 1200
        return round(principal * (cyclesTotal * rate / (1 - (1 + rate)**(-cyclesTotal)) - 1), 2)

    # Obtain list of new notes from input DF
    newNotesDF = (DF
                  .orderBy('dueDate')
                  .groupBy('loanID', 'company', 'loanName', 'interestRate', 'grade')
                  .agg(F.count('loanID').alias('cyclesTotal'), 
                       F.sum('principalScheduled').alias('principal'),
                       F.round(F.sum('fees'), 2).alias('fees'),
                       F.round(F.sum('interestScheduled') - F.sum('fees'), 2).alias('profits'),
                       F.round(F.mean('totalScheduled'), 2).alias('unitPayment'), 
                       F.add_months(F.first('dueDate'), -1).alias('startDate'))
                  .withColumn('principal', 
                              F.round(calculate_principal('unitPayment', 'interestRate', 'cyclesTotal'), 0))
                  .withColumn('cyclesRemaining', F.udf(lambda x: x, T.LongType())('cyclesTotal'))
                  .withColumn('badDebtFunds', bad_debt_funds('principal', 'grade', 'cyclesTotal'))
                  .withColumn('profits', F.col('profits') - F.col('badDebtFunds'))
                  .withColumn('amountRepayed', F.lit(0.00))
                  .cache())
    
    return newNotesDF