import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

mydata = spark.read.json("/Accounts/dmusicant/currentwork/cs348f18share/spark/jsonstuff.log")

yourdata = mydata.select("impressionId", "modelId")

ourdata = mydata.select("impressionId", "browserCookie")

yourdata.show()
ourdata.show()

joineddata = yourdata.join(ourdata,
       yourdata.impressionId == ourdata.impressionId)

joineddata.show()

results = joineddata.groupby(yourdata.impressionId) \
           .agg(psf.collect_list("modelId"))
results.show()

