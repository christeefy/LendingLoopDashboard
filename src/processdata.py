import dfanalyze

badDebtRates = {
    'A+': 0.56,
    'A': 1.83,
    'B+': 3.05,
    'B': 4.32,
    'C+': 5.46,
    'C': 6.25,
    'D+': 7.11,
    'D': 8.00,
    'E+': 8.84,
    'E': 9.64,
}

LENDING_LOOP_FEE_RATE = 1.5

def init_spark():
    # Obtain handler for spark
    return dfanalyze.init_spark()

def process_data(spark, csvString):
    # Broadcast interest rates
    badDebtRatesBroadcast = spark.sparkContext.broadcast(badDebtRates)

    # Convert Pandas DF to PySpark DF
    print 'Loading csv...'
    rawDF = dfanalyze.load_df_from_csv(spark, csvString)

    # Obtain notes
    print 'Obtaining notes...'
    notesDF = dfanalyze.obtain_notes(rawDF, badDebtRatesBroadcast)

    # Add transactions to notes
    print 'Adding transactions...'
    notesDF = dfanalyze.update_notes_with_transactions(rawDF, notesDF)

    # Analyze notes
    print 'Analyzing notes...'
    analyzedDF = dfanalyze.analyze_notes(notesDF)

    return notesDF, analyzedDF
