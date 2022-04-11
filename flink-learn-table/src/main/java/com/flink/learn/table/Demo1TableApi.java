package com.flink.learn.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Description: FLink SQL程序骨架结构
 * date: 2022/3/10 4:03 下午
 *
 * @author yangyh
 */
public class Demo1TableApi {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        String creatTableSql = "create table user_behavior(\n" +
                "    user_id bigint,\n" +
                "    item_id  bigint,\n" +
                "    category  bigint,\n" +
                "    behavior string,\n" +
                "    ts string\n" +
                ")  with (\n" +
                "    'connector' = 'filesystem',\n" +
                "    'path' = '/usr/local/yangyh/04-code/learn/flink-learn/data/state/taobao/UserBehavior-test.csv',\n" +
                "    'format' = 'csv'\n" +
                ")";
        // 创建临时表
        tableEnv.executeSql(creatTableSql);
        Table userBehaviorTable = tableEnv.from("user_behavior");
        Table behaviorCount = userBehaviorTable.groupBy("user_id").select("user_id, count(behavior) as cnt");
        String explanation = behaviorCount.explain();
        System.out.println(explanation);

        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(behaviorCount, Row.class);
        result.print();

        env.execute("table api建表、执行语句、打印数据");


    }
}
