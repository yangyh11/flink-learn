package com.yangyh.flink.java.demo05.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @description: 实现ParallelSourceFunction自定义Source
 * @author: yangyh
 * @create: 2020-01-09 20:45
 * 并行度=cpu核数*每个核的线程数
 */
public class Demo05ParallelSource implements ParallelSourceFunction<Integer> {
    private Boolean flag = true;
    private Integer count = 0;

    /**
     * 启动一个Source，大部分情况下都需要在run方法中实现一个循环产生数据
     */
    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        System.out.println("run 方法调用了");
        while (flag) {
            count++;
            ctx.collect(count);
            Thread.sleep(1000);
        }
        System.out.println("run 方法结束了");
    }

    /**
     * 执行cancel时会调用的方法。
     */
    @Override
    public void cancel() {
        flag = false;
        System.out.println("cancel 调用了");
    }
}
