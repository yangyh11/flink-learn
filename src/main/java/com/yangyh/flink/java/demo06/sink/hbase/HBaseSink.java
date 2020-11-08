package com.yangyh.flink.java.demo06.sink.hbase;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * @description:
 * @author: yangyh
 * @create: 2020-11-08 22:50
 */
public class HBaseSink extends RichSinkFunction<Map<String, String>> {

    Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "node2,node3,node4");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        conn = ConnectionFactory.createConnection(config);

    }

    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        String tableName = value.get(HBaseConstant.TABLE_NAME);
        String columnFamily = value.get(HBaseConstant.COLUMN_FAMILY);
        String rowKey = value.get(HBaseConstant.ROW_KEY);
        String columnNameList = value.get(HBaseConstant.COLUMN_NAME_LIST);

        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        String[]  columnNames= columnNameList.split(",");
        for (String columnName : columnNames) {
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName), Bytes.toBytes(value.get(columnName)));
        }
        table.put(put);
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
}
