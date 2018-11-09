#Write using the wordcount2 example.
#Questions?

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("wordcount").getOrCreate()

lines = spark.read.text("*.txt")

words1 = lines.select(psf.input_file_name(), \
                      psf.split("value","\s")) \
              		.toDF("filename", "wordlist")

words1a = words1.select(psf.split("filename","/"), "wordlist") \
			.toDF("filenamesplit", "wordlist")


words1b = words1a.select("wordlist",\
				words1a.filenamesplit[ \
            	psf.size(words1a.filenamesplit)-1])\
           .toDF( "wordlist", "realfilename")

words2 = words1b.select(psf.explode("wordlist"), "realfilename") \
              .toDF("word", "realfilename")




words2 = words2.dropDuplicates()

#.agg(psf.count("word")) \

results = words2.groupBy("word") \
                .agg(psf.collect_set("realfilename")) \
                .toDF("word", "the count")
                
df = results.select("word", "the count").toDF("word", "filename")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'

array_to_string_udf = udf(array_to_string,StringType())

df = df.withColumn('filenames',array_to_string_udf(df["filename"]))

df = df.drop("filename")

df.show()

df.repartition(1).write.save("part1out", format="csv")

