import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T


def update_notes_with_transactions(rawDF, notesDF):
    '''
    Update notesDF with completed transactions from rawDF.
    '''
    # Obtain transactions that have occured
    netTransactionsDF = (rawDF
                         .filter(F.isnull('datePaid') == 'False')
                         .groupBy('loanID')
                         .agg(F.sum('totalPaid').alias('totalPaid')))
    
    # Update noteStates
    updatedNotesStateDF = (notesDF
                           .join(netTransactionsDF, 'loanID', 'left_outer')
                           .withColumn('amountRepayed', F.col('amountRepayed') + F.col('totalPaid'))
                           .withColumn('cyclesRemaining', F.col('cyclesRemaining') - 1)
                           .drop('totalPaid')
                           .fillna(0.0)
                           .cache())
    
    return updatedNotesStateDF