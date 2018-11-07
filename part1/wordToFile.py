#Write using the wordcount2 example.
#Questions?

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

lines = spark.read.text("input.txt")

words1 = lines.select(psf.input_file_name(), \
                      psf.split("value","\s")) \
              .toDF("part1out.txt", "wordlist")
