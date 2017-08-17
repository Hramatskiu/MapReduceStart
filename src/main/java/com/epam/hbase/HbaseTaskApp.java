package com.epam.hbase;

import com.epam.hbase.job.HBaseJobCreator;
import com.epam.hbase.job.HBaseMapper;
import com.epam.hbase.job.HBaseReducer;
import com.epam.hbase.wrapper.HTableHandlerWrapper;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;

public class HbaseTaskApp {
    private static HBaseConnectionPool hBaseConnectionPool;

    public static void main(String[] args) throws Exception{
        setUpPool();
        HTableHandlerWrapper hTableHandlerWrapper = new HTableHandlerWrapper(hBaseConnectionPool);

        hTableHandlerWrapper.addFamilyToTable("sourcetest", "awesomefamily");
        List<String> names = getTableNamesList(hTableHandlerWrapper);

        /*setUpHbaseTable(config, "sourcetest", "somefamily");
        setUpHbaseTable(config, "desttest", "cpcount");*/

        initTableData(hTableHandlerWrapper, "sourcetest");

        NavigableMap<byte[], byte[]> familyMap = hTableHandlerWrapper.getFamilyMap("sourcetest", "some", "somefamily");
        for (byte[] key: familyMap.navigableKeySet()) {
            System.out.println(Bytes.toString(key) + " : " + Bytes.toString(familyMap.get(key)));
        }

        ResultScanner results = hTableHandlerWrapper.getResultScanner("sourcetest", "somefamily", "qual");

        /*if (results != null){
            System.out.println(results.next());
        }*/

        List<String> families = hTableHandlerWrapper.getTableFamilies("sourcetest");

        if (families != null) {
            families.forEach(System.out::println);
        }

        Job job = HBaseJobCreator.createJob(createConfig(), "Test",  HBaseQueryHelper.createScan(500, false),"sourcetest", "desttest");

        job.waitForCompletion(true);

        /*hTableHandlerWrapper.dropTable("sourcetest");
        hTableHandlerWrapper.dropTable("desttest");*/
    }

    private static void setUpPool() throws IOException, ServiceException{
        Configuration config = createConfig();
        HBaseAdmin.checkHBaseAvailable(config);

        hBaseConnectionPool = new HBaseConnectionPool(5, config);
    }

    private static Configuration createConfig(){
        Configuration config = HBaseConfiguration.create();
        config.addResource("hbase-site.xml");
        config.addResource("core-site.xml");

        return config;
    }

    private static void setUpHbaseTable(HTableHandlerWrapper hTableHandlerWrapper, String tableName, String familyName){
        hTableHandlerWrapper.createTable(tableName, familyName);
    }

    private static void initTableData(HTableHandlerWrapper hTableHandlerWrapper, String tableName){
        hTableHandlerWrapper.putDataInTable(tableName,"some", "somefamily", "qual", "test");
        hTableHandlerWrapper.putDataInTable(tableName, "some", "somefamily", "qualifier", "totest");
        hTableHandlerWrapper.putDataInTable(tableName, "some", "awesomefamily", "qualifier", "totest");
    }

    private static List<String> getTableNamesList(HTableHandlerWrapper hTableHandlerWrapper) {
        return hTableHandlerWrapper.getTableNamesList();
    }

    /*private static ResultScanner getResultScanner(Connection connection, String tableName) throws IOException{
        return connection.getTable(TableName.valueOf(tableName)).getScanner()
    }*/
}
