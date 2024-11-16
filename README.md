# Named Entity Analysis with Kafka
A streaming application  developed to subscribe to top news headlines using the News API. The news was streamed using Kafka into a Spark MapReduce processor, which extracted the named entities. These entities were then fed into the ELK stack for visualization

## How to Run

### Get API_KEY for New API CLient:
1. Log in into https://newsapi.org/ for and click on the "Get API Key" button
2. Copy and save the API_KEY for later use.

----------------------------------------------------------------------------------------------------------

### Install the required packages in your run time environment.
1. Intall nltk
pip install nltk

2. Install numpy
pip install numpy

3. Install news api python client
pip install newsapi-python

4. Install the kafka python clien
pip install kafka-python

Make sure kakfka and apache spark is set up in your system.

----------------------------------------------------------------------------------------------------------

### Setting Up Kafka

1. Start the ZooKeeper service (Run from kafka directory)
$ bin/zookeeper-server-start.sh config/zookeeper.properties

2. Start the Kafka broker service (Run from kafka directory)
$ bin/kafka-server-start.sh config/server.properties

3. Create a news topic (Run from kafka directory)
bin/kafka-topics.sh --create --topic news --bootstrap-server localhost:9092

4. Create a named_entities topic (Run from kafka directory)
bin/kafka-topics.sh --create --topic news_consumer --bootstrap-server localhost:9092

5. Run the python script to fetch data from news api, use the API_KEY saved in previous step
python3 news_api.py localhost:9092 news <API_KEY>


6. Start the spark job to count named entities
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 named_entities_processing.py localhost:9092 subscribe news news_consumer  ~/bda/checkpoint/

----------------------------------------------------------------------------------------------------------

### ELK

Install elastic search, logstash and kibana on your system.

1. Start Elastic search
sudo systemctl start elasticsearch.service

2. Start Kibana
sudo systemctl start kibana.service

3. Start logstash <br>
  a. copy the logstash-kafka.conf file to /etc/logstash/conf.d/ folder<br>
  b. In the output configurartion replace user name password with your elatic search password details. Username by default is usually elastic. <br>
     Also check the certificate path for your system and update it. <br>
     If ssl security is not turned on for elastic search you can remove the cacert, user and password parameters.<br>
     ```
     output {
       elasticsearch {
           hosts => ["https://localhost:9200"]
           index => "news"
           cacert => '/etc/elasticsearch/certs/http_ca.crt'
           user => 'elastic'
           password => 'elasticsearch_password'
      }
     ```

  c. Start logstash using the conf file as follows<br>
    sudo /usr/share/logstash/bin/logstash -f  /etc/logstash/conf.d/logstash-kafka.conf

