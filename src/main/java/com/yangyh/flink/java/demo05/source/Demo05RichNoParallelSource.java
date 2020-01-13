package com.yangyh.flink.java.demo05.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @description: 继承RichSourceFunction自定义Source
 * @author: yangyh
 * @create: 2020-01-09 20:38
 * 并行度 = 1
 */
public class Demo05RichNoParallelSource extends RichSourceFunction<Integer> {

    private Boolean flag = true;
    private Integer count = 0;

    //1.程序启动后，首先调用open方法，可以加载配置文件或者一些资源
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open调用了...");
        System.out.println("open关闭了...");
    }

    //2.程序调用open后，直接调用run方法产生数据
    @Override
    public void run(SourceContext ctx) throws Exception {
        System.out.println("run调用了...");
        while (flag) {
            count++;
            ctx.collect(count);
            Thread.sleep(1000);
        }
        System.out.println("run关闭了...");
    }

    //3.当cancel任务时，cancel方法被调用
    @Override
    public void cancel() {
        System.out.println("cancel调用了...");
        System.out.println("cancel关闭了...");
    }

    //4.cancel取消时，先执行cancel方法然后再执行close方法，释放一些资源
    @Override
    public void close() throws Exception {
        System.out.println("close调用了...");
        System.out.println("close关闭了...");
    }
}
