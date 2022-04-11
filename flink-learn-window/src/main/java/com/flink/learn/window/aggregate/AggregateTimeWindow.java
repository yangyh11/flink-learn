package com.flink.learn.window.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 计算窗口内平均值
 */
public class AggregateTimeWindow {
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

        stockPriceDataStreamSource.print();

        SingleOutputStreamOperator<StockPrice> windowAvg = stockPriceDataStreamSource.keyBy(stockPrice -> stockPrice.getSymbol())
                .timeWindow(Time.seconds(10))
                .aggregate(new AvgAggregate());

        windowAvg.print();

        env.execute("窗口函数 Aggregate");


    }

    static class AvgAggregate implements AggregateFunction<StockPrice, Tuple3<String, Double, Integer>, StockPrice> {

        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return Tuple3.of("", 0.0, 0);
        }

        @Override
        public Tuple3<String, Double, Integer> add(StockPrice stockPrice, Tuple3<String, Double, Integer> accumulator) {
            return Tuple3.of(stockPrice.getSymbol(), accumulator.f1 + stockPrice.getPrice(), accumulator.f2 + 1);
        }

        @Override
        public StockPrice getResult(Tuple3<String, Double, Integer> accumulator) {
            return new StockPrice(accumulator.f0, accumulator.f1 / accumulator.f2);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> acc1, Tuple3<String, Double, Integer> acc2) {
            return Tuple3.of(acc1.f0, acc1.f1 + acc2.f1, acc2.f2 + acc2.f2);
        }
    }

}
