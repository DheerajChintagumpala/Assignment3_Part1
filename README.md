# Assignment3_Part1
step 1:
For this Assignment part 1, I used NewsApi as the real-time data source.
To fetch the data form NewsApi I generated an Api key. key that I generated was  92c016ec03cd47569da84aec6a9b4c69. Visit the NewsAPI website at https://newsapi.org/ to generate the key.

step 2:
Download Apache Kafka and go through the quickstart steps:
     https://kafka.apache.org/quickstart
Now run Zookeper to start Kafka :
$ bin/zookeeper-server-start.sh config/zookeeper.properties

Open another terminal session and run Kafka broker service
$ bin/kafka-server-start.sh config/server.properties

To create a topic Open another terminal session and run:

$ bin/kafka-topics.sh --create --topic topic1 --bootstrap-server localhost:9092
$ bin/kafka-topics.sh --create --topic topic2 --bootstrap-server localhost:9092

this will create kafka topic 
(topic1 and topic2)

Step 3:
In a new terminal run the producer.py file to fetch the data form NewsApi every 60 seconds and store it to kafka topic1
$ python producer.py

To view the content on Kafka topic2 run the following command in a new terminal.
$ bin/kafka-console-consumer.sh --topic topic1 --bootstrap-server localhost:9092

step 4:
To find the named entities and its count using spark structured straming application, run the below command.
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 KafkaStream1.py localhost:9092 subscribe topic1

step 5:
will need Elasticsearch and Kibana installed to get the data from Kafka and visualize it.
https://www.elastic.co/downloads

ELK Stack :
1. Elasticsearch (At the directory of Elasticsearch) :
        ./bin/elasticsearch
2. Kibana (At the directory of Kibana) :
        ./bin/kibana
3. Logstash:
   Install Logstash and open the director in terminal.
The log_config configuration file is created in config folder of logstash folder
    Logstash requires its .conf  file to point the input and output(Kafka to elasticsearch.
input {
  kafka {
      bootstrap_servers => ["localhost:9092"]
      topics => ["topic2"]
  }
}
filter {
  grok {
    match => { "message" => '"entity":"%{DATA:entity}","count":"%{NUMBER:count}"' }
  }
}
output {
  elasticsearch {
    hosts => ["https://localhost:9200/"]
    user => "elastic"
    password => "-0EaBNEgL3=-qDI4Jexp"
    index => "newsapi"
    ssl_certificate_verification => false
  }
}

Start Logstash with  ./bin/logstash -f ./config/log_config.conf directory of Logstash.

Finally, Visualize the result on Kibana.

