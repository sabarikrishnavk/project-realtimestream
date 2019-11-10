package com.pgbde.realtime;

import java.util.*;

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
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;


/**
 * Calculate the simple moving average closing price of the four stocks
 * in a 5-minute sliding window for the last 10 minutes
 */
public class SimpleMovingAverageApp {

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

        //Create JavaStreamingContext using the spark context
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
        //reduce by window function with sliding window.
        JavaPairDStream<String, Stock> stockStream = keyMap.reduceByKeyAndWindow(new Function2<Stock, Stock, Stock>() {
            @Override
            public Stock call(Stock stock, Stock value) throws Exception {
                stock.add(value);
                return stock;
            }
        },winDuration,sldDuration);

        stockStream.print();
        //Create a pair RDD for keeping average value
        JavaPairDStream<String, Stock> avgPairStream = stockStream.mapToPair(new PairFunction<Tuple2<String, Stock>, String, Stock>() {
            @Override
            public Tuple2<String, Stock> call(Tuple2<String, Stock> tuple2) throws Exception {
                Stock stock1 = tuple2._2;
                Stock avgStock = new Stock();
                avgStock.setSymbol(stock1.getSymbol());
                avgStock.setCount(stock1.getCount());

                PriceData data = new PriceData();
                data.setClose(stock1.getPriceData().getClose()/stock1.getCount());
                data.setOpen(stock1.getPriceData().getOpen()/stock1.getCount());
                data.setHigh(stock1.getPriceData().getHigh()/stock1.getCount());
                data.setLow(stock1.getPriceData().getLow()/stock1.getCount());
                avgStock.setPriceData(data);
                return new Tuple2<>(tuple2._1,avgStock);
            }
        });

        //Print the results.
        //Create a tuple with volume as Symbol and avg closing
        JavaPairDStream<String,Double> avgClosingStream=avgPairStream.mapToPair(new PairFunction<Tuple2<String, Stock>,String,Double>() {
            @Override
            public Tuple2<String,Double> call(Tuple2<String, Stock> stockTuple2) throws Exception {
                return new Tuple2<>(stockTuple2._2.getSymbol(),stockTuple2._2.getPriceData().getClose());
            }
        }
        );
        avgClosingStream.print();


        //Print the output into the file in Symbol, Double (simple moving average)  format
        avgClosingStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Double>>() {
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
