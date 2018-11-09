#Write using the wordcount2 example.
#Questions?

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("wordcount").getOrCreate()

lines = spark.read.text("/Accounts/courses/cs348/gutenberg/*.txt")

data = lines.select(psf.input_file_name(), \
                      psf.split("value","\s")) \
              		.toDF("filename", "wordlist")

data = data.select(psf.split("filename","/"), "wordlist") \
			.toDF("filenamesplit", "wordlist")


data = data.select("wordlist",\
				words1a.filenamesplit[ \
            	psf.size(words1a.filenamesplit)-1])\
           .toDF( "wordlist", "realfilename")

data = data.select(psf.explode("wordlist"), "realfilename") \
              .toDF("word", "realfilename")




data = data.dropDuplicates() # This is necessary for multiples in same file



data = data.groupBy("word") \
                .agg(psf.collect_set("realfilename")) \
                .toDF("word", "the count")
                
df = data.select("word", "the count").toDF("word", "filename")


'''Our program wasn't working due to the whole
	"csv's don't do lists" thing. we solved this by turning the list
	into a string, with some code we found from stackoverflow. it's simple
	code, and we get what it does.'''
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'

array_to_string_udf = udf(array_to_string,StringType())

# Ok, back to code we actually wrote 



df = df.withColumn('filenames',array_to_string_udf(df["filename"]))

df = df.drop("filename")

df.show()

df.repartition(1).write.save("part1out", format="csv")

