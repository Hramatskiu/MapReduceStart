package com.epam.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HbaseTaskApp {
    public static void main(String[] args) throws Exception{
        Configuration config = createConfig();

        setUpHbaseTable(config, "sourcetest", "somefamily");
        setUpHbaseTable(config, "desttest", "cpcount");

        initTableData(config, "sourcetest");

        Job job = createJob(config, "Test", "sourcetest", "desttest");

        job.waitForCompletion(true);
    }

    private static Configuration createConfig(){
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "svqxbdcn6hdp25n1.pentahoqa.com:2181");

        return config;
    }

    private static Job createJob(Configuration configuration, String jobName, String sourceTable, String destTable) throws IOException{
        Job job = new Job(configuration,jobName);
        job.setJarByClass(HbaseTaskApp.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(sourceTable, scan, HBaseMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(destTable, HBaseReducer.class, job);
        job.setNumReduceTasks(1);

        return job;
    }

    private static void setUpHbaseTable(Configuration configuration, String tableName, String familyName){
        try(Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));

            if (!admin.tableExists(tableDescriptor.getTableName())){
                admin.createTable(tableDescriptor);
            }
        }
        catch (IOException ex){
            //logging
        }
    }

    private static void initTableData(Configuration configuration, String tableName){
        setUpTableData(configuration, tableName, "some", "somefamily", "qual", "test");
        setUpTableData(configuration, tableName, "some", "somefamily", "qual", "totest");
    }

    private static void setUpTableData(Configuration configuration, String tableName, String rowName, String familyName, String qualifier, String value){
        try(Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            HBaseUtil.tryToPutData(rowName, familyName, qualifier, value, table);
        }
        catch (IOException ex){
            //logging
        }
    }
}
