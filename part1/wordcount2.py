#Sample from Dave Musicant

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

#lines = spark.read.text("gutenberg")
lines = spark.read.text("/Accounts/dmusicant/Downloads/gutenberg/gutenberg/*/b*")
#lines.show()

words1 = lines.select(psf.input_file_name(), \
                      psf.split("value","\s")) \
              .toDF("filename", "wordlist")

words1a = words1.select(\
        psf.split("filename","/"), "wordlist") \
           .toDF("filenamesplit", "wordlist")

words1b = words1a.select(\
            words1a.filenamesplit[ \
            psf.size(words1a.filenamesplit)-1] \
                         , "wordlist") \
           .toDF("realfilename", "wordlist")

words2 = words1b.select("realfilename",psf.explode("wordlist")) \
              .toDF("realfilename","word")

results = words2.groupBy("realfilename","word") \
                .agg(psf.count("word")) \
                .toDF("filename","word","thecount")

#results.show()

results.repartition(1).write.save("output", format="csv")
