package com.flink.learn.window.aggregate;

import lombok.Data;

@Data
public class StockPrice {
    private String symbol;
    private Double price;

    public StockPrice() {
    }

    public StockPrice(String symbol, Double price) {
        this.symbol = symbol;
        this.price = price;
    }


}
