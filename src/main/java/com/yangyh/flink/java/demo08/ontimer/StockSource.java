package com.yangyh.flink.java.demo08.ontimer;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class StockSource implements SourceFunction<StockPrice> {
    private boolean isRunning = true;
    private String path;
    private InputStream inputStream;

    StockSource(String path){
        this.path = path;
    }

    @Override
    public void run(SourceContext<StockPrice> sourceContext) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
        inputStream = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split(",");
            LocalDateTime dateTime = LocalDateTime.parse(itemStrArr[1] + " " + itemStrArr[2], formatter);
            long eventTs = Timestamp.valueOf(dateTime).getTime();

            StockPrice stockPrice = new StockPrice(itemStrArr[0], eventTs, Double.valueOf(itemStrArr[2]), Long.valueOf(itemStrArr[3]));
            sourceContext.collect(stockPrice);
            TimeUnit.SECONDS.sleep(5);
        }
    }

    @Override
    public void cancel() {

    }
}
