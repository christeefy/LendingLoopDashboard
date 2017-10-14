import findspark
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

import pandas as pd
import math


# Initialise and return a Spark Session object.
def init_spark():
    findspark.init()

    spark = (pyspark.sql.SparkSession
        .builder
        .appName('LendingLoopWebApp')
        .getOrCreate())

    return spark


def load_df_from_csv(spark, csvString):
    # Read raw CSV file into Pandas
    rawPandasDF = pd.read_csv(csvString)

    # Create schema for PySpark DF
    schema = T.StructType([
        (T.StructField('Payment Type', T.StringType())),
        (T.StructField('Loan Id', T.IntegerType())),
        (T.StructField('Company', T.StringType())),
        (T.StructField('Loan Name', T.StringType())),
        (T.StructField('Interest Rate', T.FloatType())),
        (T.StructField('Risk Band', T.StringType())),
        (T.StructField('Interest Scheduled', T.FloatType())),
        (T.StructField('Principal Scheduled', T.FloatType())),
        (T.StructField('Total Scheduled', T.FloatType())),
        (T.StructField('Interest Owed', T.FloatType())),
        (T.StructField('Principal Owed', T.FloatType())),
        (T.StructField('Total Owed', T.FloatType())),
        (T.StructField('Interest Paid', T.FloatType())),
        (T.StructField('Principal Paid', T.FloatType())),
        (T.StructField('Total Paid', T.FloatType())),
        (T.StructField('Fees Paid to Loop', T.FloatType())),
        (T.StructField('Due Date', T.StringType())),
        (T.StructField('Date Paid', T.StringType())),
        (T.StructField('Status', T.StringType())),
    ])

    # Convert pandas DF to PySpark DF
    rawDF = spark.createDataFrame(rawPandasDF, schema)

    # Camel case titles
    camelCaseDict = {title: title[0].lower() + title.replace(' ', '')[1:] for title in rawDF.columns}

    # Simplify certain column titles
    camelCaseDict['Fees Paid to Loop'] = 'fees'
    camelCaseDict['Risk Band'] = 'grade'
    camelCaseDict['Loan Id'] = 'loanID'

    # Camelcase column titles
    rawDF = rawDF.select([F.col(title).alias(camelCaseDict[title]) for title in camelCaseDict.keys()])

    return rawDF


def obtain_notes(DF, badDebtRatesBroadcast):
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
        rate = badDebtRatesBroadcast.value[grade] / 1200
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


def update_notes_with_transactions(rawDF, notesDF):
    '''
    Update notesDF with completed transactions from rawDF.
    '''
    # Obtain transactions that have occured
    netTransactionsDF = (rawDF
                         .filter(F.isnull('datePaid') == 'False')
                         .groupBy('loanID')
                         .agg(F.sum('totalPaid').alias('totalPaid'), 
                              F.count('loanID').alias('numPayments')))
    
    # Update noteStates
    updatedNotesStateDF = (notesDF
                           .join(netTransactionsDF, 'loanID', 'left_outer')
                           .fillna(0.0)
                           .withColumn('amountRepayed', F.col('amountRepayed') + F.col('totalPaid'))
                           .withColumn('cyclesRemaining', F.col('cyclesRemaining') - F.col('numPayments'))
                           .drop('totalPaid', 'numPayments')
                           .cache())
    
    return updatedNotesStateDF


def analyze_notes(DF):
    '''
    Analyze notes by calculating payment breakdowns based on pre-specified categories.
    '''
    # Define helper function
    def _min_max(x, minVal, maxVal):
        assert minVal <= maxVal
        return max(minVal, min(x, maxVal))
    
    def _capitalise_first_letter(string):
        return ''.join([char.upper() if i == 0 else char for i, char in enumerate(string)])
    
    # Define udfs
    @F.udf(returnType=T.FloatType())
    def calc_received(val, maxVal, *args):
        return _min_max(val - sum(args), 0., maxVal)
    
    @F.udf(returnType=T.FloatType())
    def calc_next_payment(val, maxVal, unitPayment, *args):
        # Handle case when nextPMT spills over from previous category
        if (val - sum(args) < 0):
            return _min_max(unitPayment - (sum(args) - val), 0., unitPayment)
        
        remainder = _min_max(maxVal - (val - sum(args)), 0., maxVal)
        return min(remainder, unitPayment)
    
    @F.udf(returnType=T.FloatType())
    def calc_outstanding(val, maxVal, unitPayment, *args):
        return _min_max(maxVal - (val + unitPayment - sum(args)), 0., maxVal)
    
    @F.udf(returnType=T.IntegerType())
    def months_to_breakeven(nextPrincipalPayment, principalOutstanding, unitPayment):
        return int(math.ceil((nextPrincipalPayment + principalOutstanding) / unitPayment))
    
    @F.udf(returnType=T.BooleanType())
    def completed(cyclesRemaining):
        return not cyclesRemaining
        
    
    # Add relevant columns to notesDF
    categories = ['fees', 'principal', 'badDebtFunds', 'profits']
    
    # Add `received`, `next_payment` and `outstanding` columns programmatically
    for i, category in enumerate(categories):
        DF = (DF
              .withColumn('{}Received'.format(category) if category != 'fees' else 'feesPaid', 
                          calc_received('amountRepayed', category, *categories[:i]))
              .withColumn('next{}Payment'.format(_capitalise_first_letter(category)), 
                          calc_next_payment('amountRepayed', category, 'unitPayment', *categories[:i]))
              .withColumn('{}Outstanding'.format(category), 
                          calc_outstanding('amountRepayed', category, 'unitPayment', *categories[:i])))
        
    # Add `monthsToBreakeven` and `completed` column
    DF = (DF
          .withColumn('monthsToBreakeven', 
                      months_to_breakeven('nextPrincipalPayment',
                                          'principalOutstanding',
                                          'unitPayment'))
          .withColumn('completed', completed('cyclesRemaining'))
          .orderBy(['principal', 'interestRate'], 
                   ascending=[1, 1])
          .cache())
    
    return DF


