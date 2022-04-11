package com.yangyh.flink.java.demo08.ontimer;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

public class IncreaseAlertFunction extends KeyedProcessFunction<String, StockPrice, String> {
    private long intervalMills;
    private ValueState<Double> lastPriceState;
    private ValueState<Long> currentTimerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> lastPriceStateDescriptor = new ValueStateDescriptor<>("lastPrice", Double.class);
        ValueStateDescriptor<Long> currentTimerStateDescriptor = new ValueStateDescriptor<>("currentTimer", Long.class);

        lastPriceState = getRuntimeContext().getState(lastPriceStateDescriptor);
        currentTimerState = getRuntimeContext().getState(currentTimerStateDescriptor);
    }

    @Override
    public void processElement(StockPrice stockPrice, Context context, Collector<String> collector) throws Exception {
        Double lastPrice = lastPriceState.value();
        if (null == lastPrice) {
            lastPrice = stockPrice.getPrice();
        } else {
            long currentTimer;
            if (null == currentTimerState.value()) {
                currentTimer = 0;
            } else {
                currentTimer = currentTimerState.value();
            }

            if (stockPrice.getPrice() < lastPrice) {
                // 股票价格降低，删除Timer，否则一直保留Timer
                context.timerService().deleteEventTimeTimer(currentTimer);
                currentTimerState.clear();
            } else if (currentTimer == 0) {
                long timerTs = context.timestamp() + intervalMills;
                context.timerService().registerEventTimeTimer(timerTs);
                currentTimerState.update(timerTs);
            }
        }
        lastPriceState.update(lastPrice);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HHmmss");
        String msg = formatter.format(timestamp) + ", symbol: " + ctx.getCurrentKey() +
                "价格上涨了 " + intervalMills;

    }
}
