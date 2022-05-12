from pyspark .sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark by example 2").getOrCreate()
    print(spark)

    # # create the data frame from rdd
    # columns = ["language",'user_count']
    # data = [('java','2000'),('python','100000'),("scala",'3000')]
    # one easy way to manually create pyspark data frame is from an existing rdd first lets crate a spark rdd from a collection list by calling parallelize() funcation from spark context we would nedd this rdd object for all our example below.

    #
    # rdd =spark.sparkContext.parallelize(data)
    # print(rdd.collect())

    # using toDF() funcation

    # pyspark rdd toDF() method is used to crate a data frame from existing rdd.since the rdd dosent have the column tha data frame is created the default column name"-_1" and "_2" as we have two columns


    # df = rdd.toDF()
    # df.printSchema()
    # df.show()

    # columns = ["language","user_count"]
    # df2 = rdd.toDF(columns)
    # df2.printSchema()
    # df2.show()

    # crate the data frame ()fro9m spark sessition

    # # using createDataFrame() from SparkSession is another way to create the manually and it takes rdd object as asn argumnet and chain with toDF() to speciy tha neme of the columns
    # df3 = spark.createDataFrame(rdd).toDF(*columns)
    # df3.show()


    # crate the dat afrme with schema
    # if we wnt to spaecify the columns name along with their data types schema first and then assign this while creating the Struct schema first and then assign this while creating the data frame
    data = [('james','ram','smith',36636,'M',3000),
            ("michael",'rose','sam',40288,'M',4000),
            ("robert","sham","williams",42114,'M',40000),
            ('maria','anne',"jones",39192,'F',40000),
            ('jen','mary',"brown",3443,"F",-1)]

    schema = StructType([
                            StructField('firstname',StringType(),True),
                            StructField('middle_name',StringType(),True),
                            StructField('lastname',StringType(),True),
                            StructField('id',IntegerType(),True),
                            StructField('gender',StringType(),True),
                            StructField('salary',IntegerType(),True)])

    df4 = spark.createDataFrame(data=data,schema=schema)
    df4.printSchema()
    df4.show()

    # create the data fram from csv file
    # df2 = spark.read.csv(file path)
    # create from text file
    # df2 = spark.read.txt("file path")\
    # creating from json file
    #df2.spark.read.json("file path")
    


