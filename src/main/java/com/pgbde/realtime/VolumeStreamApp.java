package com.pgbde.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * Calculate the trading volume(total traded volume) of the four stocks
 * every 10 minutes and decide which stock to purchase out of the four stocks
 */

public class VolumeStreamApp {
    public static void start(String broker, String topic,String outputPath,long batchinterval,long windowinterval, long slidinginterval ) throws InterruptedException {

        Duration batDuration = Durations.minutes(batchinterval);
        Duration winDuration = Durations.minutes(windowinterval);
        Duration sldDuration = Durations.minutes(slidinginterval);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", broker);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", JsonDeserializer.class);
        kafkaParams.put("group.id", SparkStreamApplication.groupId);

        Set<String> topicSet = new HashSet<String>(Arrays.asList(topic.split(",")));

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("SparkApplication");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, batDuration);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(topicSet, kafkaParams));

        //Create a pair D stream with Symbol and stock data.
        JavaPairDStream<String,Stock> keyMap = messages.flatMapToPair(new PairFlatMapFunction<ConsumerRecord<String, String>, String, Stock>() {
            @Override
            public Iterator<Tuple2<String, Stock>> call(ConsumerRecord<String, String> record) throws Exception {
                List<Tuple2<String, Stock>> list = new ArrayList<Tuple2<String, Stock>>();

                ObjectMapper mapper = new ObjectMapper();
                Stock stock =  mapper.convertValue(record.value(), Stock.class);
                list.add(new Tuple2<String, Stock>(stock.getSymbol(),stock));
                return list.iterator();
            }
        });
        keyMap.print();

        //Reduce by 10min batch
        JavaPairDStream<String, Stock> tradingVolumeStream = keyMap.reduceByKey(new Function2<Stock, Stock, Stock>() {
            @Override
            public Stock call(Stock stock, Stock value) throws Exception {
                stock.add(value);
                return stock;
            }
        });

        tradingVolumeStream.print();

        //Create a tuple with volume as key and stock
        JavaPairDStream<Double, String> resultSwapped=tradingVolumeStream.mapToPair(new PairFunction<Tuple2<String, Stock>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<String, Stock> stockTuple2) throws Exception {
                return new Tuple2<>(stockTuple2._2.getPriceData().getVolume(),stockTuple2._2.getSymbol());
            }
        }
        );
        //sort the key with volume.
        JavaPairDStream<Double, String> sorted=resultSwapped.transformToPair(new Function<JavaPairRDD<Double,String>, JavaPairRDD<Double,String>>() {
            public JavaPairRDD<Double, String> call(JavaPairRDD<Double, String> v1)
                    throws Exception {
                return v1.sortByKey(false);
            }
        });
        sorted.print();


        //Print the output into the file in Symbol, Double(volume) format
        sorted.mapToPair(new PairFunction<Tuple2<Double, String>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<Double, String> doubleStringTuple2) throws Exception {
                return new Tuple2<String,Double>(doubleStringTuple2._2,doubleStringTuple2._1);
            }
        }).foreachRDD( new VoidFunction<JavaPairRDD<String,Double>>() {
            private static final long serialVersionUID = 6767679;
            public void call(JavaPairRDD<String,Double> t)
                    throws Exception {
                t.coalesce(1).saveAsTextFile(outputPath+java.io.File.separator + System.currentTimeMillis());
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
