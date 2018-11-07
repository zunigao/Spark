#Write using the wordcount2 example.
#Questions?

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("wordcount").getOrCreate()

lines = spark.read.text("*.txt")

words1 = lines.select(psf.input_file_name(), \
                      psf.split("value","\s")) \
              .toDF("filename", "wordlist")

words1a = words1.select(\
        psf.split("filename","/"), "wordlist") \
           .toDF("filenamesplit", "wordlist")


words1b = words1a.select("wordlist",\
				words1a.filenamesplit[ \
            	psf.size(words1a.filenamesplit)-1])\
           .toDF( "wordlist", "realfilename")

words2 = words1b.select(psf.explode("wordlist"), "realfilename") \
              .toDF("word", "realfilename")
#words2.show()
#.agg(psf.count("word")) \

results = words2.groupBy("word","realfilename") \
                .agg(psf.collect_list("realfilename")) \
                .toDF("word","realfilename", "the count")
                
results2 = results.select("word", "the count").toDF("word", "filename")



results2.show()

#words1.repartition(1).write.save("part1out", format="txt")

