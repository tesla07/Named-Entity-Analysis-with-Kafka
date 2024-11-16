"""
 Consumes messages from  a topic in Kafka and does wordcount of named entities and writes to another kafka topic
 Usage: named_entities_processing.py <bootstrap-servers> <subscribe-type> <read_topic> <write_topic> <checkpoint_folder>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. 
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   <read_topic> The name of the topic to read from
   <write_topic> The name of the kafka topic to write into
   <checkpoint_folder> Any accessible folder path where the checkpoint data will be written

 Run the example
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 named_entities_processing.py localhost:9092 subscribe news news_consumer  ~/bda/checkpoint/

"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, udf, to_json, struct, col
from pyspark.sql.types import *

import nltk
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from nltk.chunk import ne_chunk

# Extract named entities from text
def extract_named_entities(text):
    words = word_tokenize(text)
    # tag the words
    tagged_words = pos_tag(words)
    named_entities = ne_chunk(tagged_words)
    
    named_entities_list = []
    for entity in named_entities:
        if isinstance(entity, nltk.tree.Tree):
            entity_name = " ".join([word for word, tag in entity.leaves()])
            named_entities_list.append(entity_name)
    
    return named_entities_list

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("""
        Usage: named_entities_processing.py <bootstrap-servers> <subscribe-type> <read_topic> <write_topic> <checkpoint_folder>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]
    write_topic = sys.argv[4]
    folder_path = sys.argv[5]

    spark = SparkSession\
        .builder\
        .appName("NamedEntityCounter")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input news from kafka
    news = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .option("failOnDataLoss", "false") \
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    
    # Define the extract_named_entities as a user defined fucntion
    extract_entities_udf = udf(extract_named_entities, ArrayType(StringType()))

    # Apply UDF to extract named entities
    df = news.withColumn("named_entities", extract_entities_udf(news["value"]))


    # Flatten the named entities array
    news = df.withColumn("named_entity", explode("named_entities")).select("named_entity")

    # Generate running word count of named entities
    nameEntitiesCounts = news.groupBy('named_entity').count()

    # Lets rename the columns to "key" and "value" as kafka looks for key and value
    entities = nameEntitiesCounts.select(col("named_entity").alias("key"), col("count").alias("value"))

    # Convert DataFrame to JSON string with value
    json_df = nameEntitiesCounts.select(to_json(struct("*")).alias("value"))

    # Write the counts to Kafka topic2
    query =  json_df\
            .writeStream \
            .outputMode("complete") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrapServers) \
            .option("topic", write_topic) \
            .option("checkpointLocation", folder_path) \
            .start()
    
    query.awaitTermination()
