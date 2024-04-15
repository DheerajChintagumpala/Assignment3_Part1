#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Consumes messages from one or more topics in Kafka and does wordcount.
 Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
   comma-separated list of host:port.
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   'subscribePattern'.
   |- <assign> Specific TopicPartitions to consume. Json string
   |  {"topicA":[0,1],"topicB":[2,4]}.
   |- <subscribe> The topic list to subscribe. A comma-separated list of
   |  topics.
   |- <subscribePattern> The pattern used to subscribe to topic(s).
   |  Java regex string.
   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
   |  specified for Kafka source.
   <topics> Different value format depends on the value of 'subscribe-type'.

 Run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
    host1:port1,host2:port2 subscribe topic1,topic2`
"""
from __future__ import print_function
import sys
import spacy
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,struct,to_json
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType

# Load SpaCy English NER model and stopwords
nlp = spacy.load('en_core_web_sm')
stopwords = spacy.lang.en.stop_words.STOP_WORDS

# Function to extract named entities from text
def extract_entities(text):
    doc = nlp(text)
    entities = [ent.text for ent in doc.ents if ent.label_ in ['PERSON', 'ORG', 'GPE']]
    return entities

# UDF to apply extract_entities function to DataFrame column
extract_entities_udf = udf(extract_entities, ArrayType(StringType()))

# Function to clean, filter, remove stopwords, and lemmatize tokens
def process_token(token):
    # Remove punctuation and special characters
    cleaned_token = re.sub(r'[^a-zA-Z]', '', token)
    # Exclude tokens with non-alphabetic characters and stopwords
    if cleaned_token.isalpha() and cleaned_token.lower() not in stopwords:
        # Perform stemming and lemmatization
        token_doc = nlp(cleaned_token.lower())
        processed_token = token_doc[0].lemma_
        return processed_token
    else:
        return None  # Exclude token

# UDF to apply process_token function to DataFrame column
process_token_udf = udf(process_token, StringType())

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from Kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    # Extract named entities from text
    named_entities = lines.select(extract_entities_udf('value').alias('named_entities'))

    # Explode named entities into separate rows
    exploded_entities = named_entities.select(explode('named_entities').alias("entity"))

    # Tokenize the entity strings
    tokenized_entities = exploded_entities.withColumn("token", explode(split(exploded_entities["entity"], ' ')))

    # Clean, filter, remove stopwords, and lemmatize tokens
    processed_tokens = tokenized_entities.withColumn("processed_token", process_token_udf(tokenized_entities["token"])).filter("processed_token is not null")

    # Generate running word count of processed tokens
    token_word_counts = processed_tokens.groupBy('processed_token').count()
    entityCount= token_word_counts.selectExpr("CAST(processed_token AS STRING) AS entity", "CAST(count AS STRING) AS count")
    

    # Start running the query that writes the results to Kafka topic
    query = entityCount\
            .selectExpr("to_json(struct(*)) AS value")\
            .writeStream\
            .outputMode('complete')\
            .format('kafka')\
            .option("kafka.bootstrap.servers", bootstrapServers)\
            .option("topic", "topic2")\
            .option("checkpointLocation", "/Users/apple/Downloads/ASS3/kafka_2.13-3.7.0/checkpointlocation")\
            .start()
    query.awaitTermination()





