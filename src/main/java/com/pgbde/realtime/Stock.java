package com.pgbde.realtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.sql.Timestamp;

//
//{"symbol":"LTC","timestamp":"2019-05-14 09:43:00",
//        "priceData":{
//        "close":90.39,"high":90.39,"low":90.23,
//        "open":90.23,"volume":-6370.7300000000005}
//}
public class Stock  implements Serializable {
    private String symbol;
    private int count =1;
    private String timestamp;
    private PriceData priceData;

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        String s = symbol + " : " + getPriceData().getHigh() + " : " +getPriceData();
        try {
            s = mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return s;
    }

    /**
     * Method to keep average
     * @param stock2
     */
    public void add(Stock stock2) {
        this.count++;
        PriceData data = this.getPriceData();
        data.setTotal( data.getTotal() + stock2.getPriceData().getTotal());
        data.setHigh( (data.getHigh()+stock2.getPriceData().getHigh()));
        data.setLow( (data.getLow()+stock2.getPriceData().getLow()));
        data.setOpen( (data.getOpen()+ stock2.getPriceData().getOpen()));
        data.setClose( (data.getClose()+stock2.getPriceData().getClose()));

        //Take the absolute value for calculation
        data.setVolume(Math.abs(data.getVolume())+ Math.abs(stock2.getPriceData().getVolume()));
    }


    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
    public PriceData getPriceData() {
        return priceData;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setPriceData(PriceData priceData) {
        this.priceData = priceData;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

}
class PriceData implements Serializable {
    private double total;
    private double close;
    private double high;
    private double low;
    private double open;
    private double volume;

    public void setTotal(double total) {
        this.total = total;
    }

    public double getTotal() {
        return total;
    }

    public void setClose(double close) {
        this.close = close;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public void setVolume(double volume) {
        this.volume = Math.abs(volume);
    }

    public double getClose() {
        return close;
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    public double getOpen() {
        return open;
    }

    public double getVolume() {
        return volume;
    }
}
