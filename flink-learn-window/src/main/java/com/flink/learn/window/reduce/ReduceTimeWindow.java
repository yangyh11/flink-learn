package com.flink.learn.window.reduce;

import com.flink.learn.window.aggregate.StockPrice;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 计算窗口内的累加值
 */
public class ReduceTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<StockPrice> stockPriceDataStreamSource = env.addSource(new SourceFunction<StockPrice>() {
            @Override
            public void run(SourceContext<StockPrice> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(new StockPrice("中免", new Random().nextDouble()));
                    TimeUnit.SECONDS.sleep(2);
                }
            }
            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<StockPrice> windowSum = stockPriceDataStreamSource.keyBy(stockPrice -> stockPrice.getSymbol())
                .timeWindow(Time.seconds(10))
                .reduce((s1, s2) -> new StockPrice(s1.getSymbol(), s1.getPrice() + s2.getPrice()));

        windowSum.print();

        env.execute("窗口函数 reduce");
    }
}
