package com.pgbde.realtime;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Main program to invoke all listen to a "stockData" kafka stream and
 * invoke sub programs with different logic.
 *
 * (BTC,{"symbol":"BTC","count":1,"timestamp":"2019-11-10 01:33:00","priceData":{"total":0.0,"close":8786.94,"high":8786.95,"low":8784.8,"open":8784.81,"volume":18474.365}})
 * (ETH,{"symbol":"ETH","count":1,"timestamp":"2019-11-10 01:33:00","priceData":{"total":0.0,"close":184.11,"high":184.11,"low":184.09,"open":184.1,"volume":867.259}})
 * (LTC,{"symbol":"LTC","count":1,"timestamp":"2019-11-10 01:33:00","priceData":{"total":0.0,"close":61.11,"high":61.11,"low":61.11,"open":61.11,"volume":335.221}})
 * (XRP,{"symbol":"XRP","count":1,"timestamp":"2019-11-10 01:33:00","priceData":{"total":0.0,"close":0.2779,"high":0.278,"low":0.2779,"open":0.278,"volume":2450.73}})
 * (BTC,{"symbol":"BTC","count":1,"timestamp":"2019-11-10 01:34:00","priceData":{"total":0.0,"close":8783.09,"high":8787.21,"low":8783.09,"open":8786.94,"volume":16169.987}})
 *
 */
public class SparkStreamApplication {


    public static String groupId ="stockData-sabari";// "stockgroup";

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org")
                .setLevel(Level.WARN);
        Logger.getLogger("akka")
                .setLevel(Level.WARN);

        if (args.length < 7 ) {

            System.out.println(" Program Input missing:  program (sma,volume, profit) ,broker , topic name , batch interval, window interval , sliding interval (in minutes) ," +
                    "Output file -> " +
                    "eg: sma localhost:9092 stockData 5 10 5 output/sma");
            System.exit(0);
        }

        String program = args[0];


        String brokers = args[1];//"localhost:9092";
        String topics = args[2];//"stockData";// "stockstream";

        int batchinterval = Integer.parseInt(args[3]);//  5;
        int windowinterval =Integer.parseInt(args[4]);// 10;
        int slidinginterval =Integer.parseInt(args[5]);// 5 ;
        String outputPath = args[6];

        if("sma".equals(program) ){
            System.out.println("Executing SimpleMovingAverageApp ");
            SimpleMovingAverageApp.start(brokers,topics,outputPath,batchinterval,windowinterval,slidinginterval);
        }
        if("volume".equals(program) ){
            System.out.println("Executing VolumeStreamApp ");
            VolumeStreamApp.start(brokers,topics,outputPath,batchinterval,windowinterval,slidinginterval);
        }

        if("profit".equals(program) ){
            System.out.println("Executing MaxProfitApp ");
            MaxProfitApp.start(brokers,topics,outputPath,batchinterval,windowinterval,slidinginterval);
        }

    }
}
