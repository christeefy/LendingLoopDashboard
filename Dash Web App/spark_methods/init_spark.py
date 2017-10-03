import findspark
import pyspark


def init_spark():
    '''
    Initialise and return a Spark Session object.
    '''
    findspark.init()

    return (pyspark.sql.SparkSession
            .builder.appName('LendingLoopWebApp')
            .getOrCreate())