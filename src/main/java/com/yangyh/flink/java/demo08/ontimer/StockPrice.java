package com.yangyh.flink.java.demo08.ontimer;

import lombok.Data;

@Data
public class StockPrice {

    private String symbol;
    private Long ts;
    private Double price;
    private Long volume;

    public StockPrice(String symbol, Long ts, Double price, Long volume) {
        this.symbol = symbol;
        this.ts = ts;
        this.price = price;
        this.volume = volume;
    }
}
