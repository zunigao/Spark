import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

clicks = spark.read.json("clicks.log")
impressions = spark.read.json("impressions.log")


clicks = clicks.select("adId")
impressions = impressions.select("referrer","adId")

#yourdata.show()
#ourdata.show()

combinedData = impressions.join(clicks, impressions.adId == clicks.adId)

#combinedData = combinedData.select("referrer", "adId")


combinedData.show()


"""
joineddata = yourdata.join(ourdata,
       yourdata.impressionId == ourdata.impressionId)

joineddata.show()

results = joineddata.groupby(yourdata.impressionId) \
           .agg(psf.collect_list("modelId"))
results.show()

"""