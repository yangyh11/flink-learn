package com.flink.learn.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yangyh
 */
public class RowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.100.104", 9000);

        socketTextStream.print();

        SingleOutputStreamOperator<Row> rowStream = socketTextStream.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                Row row = Row.withNames();
                row.setField("name", "yangyh");
                row.setField("id", "1");
                return row;
            }
        });

        SingleOutputStreamOperator<Row> map = rowStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row r) throws Exception {
                Row newRow = Row.withNames();
                newRow.setField("filed_name", "filed_value");
                // ...
                r.getFieldNames(true).forEach(name -> {
                    newRow.setField(name, r.getField(name));
                });
                return newRow;
            }
        });

        map.print();
        env.execute("Row Demo");

    }
}
