# project-realtimestream


# Create a kafka stream in local

Download apache kafka

cd Downloads/kafka_2.12-2.3.0

Run the following command in different windows
bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic stockstream

> Paste the following json string as input to the stream in the producer window.
>(BTC,{"symbol":"BTC","count":1,"timestamp":"2019-11-10 01:34:00","priceData":{"total":0.0,"close":8783.09,"high":8787.21,"low":8783.09,"open":8786.94,"volume":16169.987}})
>


# Spark program with kafka stream

Main class SparkStreamApplication accepts following mandatory parameters in arguments
> program name (
    Simple moving average -sma,
    Volume - volume, 
    Maximum profit -profit ,
    Broker name , 
    topic name , 
    batch interval (in minutes), 
    window interval (in minutes), 
    sliding interval (in minutes) ,
     Output folder name
     "eg: sma 52.55.237.11:9092 stockData 5 10 5 output/sma
    Based on the program name different classes are invoked.
    “sma” invokes  SimpleMovingAverageApp
    “profit” invokes MaxProfitApp 
    “volume” invokes VolumeStreamApp


Run the following commands to execute the program from command
 for simple moving average, maximum  profile and trading volume.
> mvn clean package
> java -cp target/SparkApplication-jar-with-dependencies.jar com.pgbde.realtime.SparkStreamApplication sma localhost:9092 stockstream 5 10 5 output/sma
> java -cp target/SparkApplication-jar-with-dependencies.jar com.pgbde.realtime.SparkStreamApplication profit localhost:9092 stockstream 5 10 5 output/profit
> java -cp target/SparkApplication-jar-with-dependencies.jar com.pgbde.realtime.SparkStreamApplication volume localhost:9092 stockstream 5 10 5 output/volume
