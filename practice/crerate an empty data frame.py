from pyspark.sql import SparkSession

from pyspark.sql.types import *
from pyspark.sql.window import *
if __name__ == "__main__":
    spark=SparkSession.builder.appName("practice").master("local").getOrCreate()
    # print(spark)

    #1) create the empty rdd in pyspark
    # create the empty rdd by using emptyRDD() of SparkContext for example
    # spark.sparkComtext.emptyrdd()

    # create the empty rdd


