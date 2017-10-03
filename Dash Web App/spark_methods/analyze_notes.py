import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

def analyze_notes(DF):
    '''
    Analyze DF by calculating payment breakdowns based on pre-specified categories.
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
        
    # Add `monthsToBreakeven` column
    DF = (DF.withColumn('monthsToBreakeven', 
                        months_to_breakeven('nextPrincipalPayment',
                                            'principalOutstanding',
                                            'unitPayment'))
          .orderBy(['feesPaid', 'principal', 'principalOutstanding', 'interestRate', 'cyclesTotal'], 
                   ascending=[1, 1, 0, 1, 1])
          .cache())
    
    return DF