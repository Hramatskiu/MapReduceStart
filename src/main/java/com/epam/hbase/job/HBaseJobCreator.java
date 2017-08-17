package com.epam.hbase.job;

import com.epam.hbase.HbaseTaskApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HBaseJobCreator {
    private HBaseJobCreator(){}

    public static Job createJob(Configuration configuration, String jobName, Scan scan, String sourceTable, String destTable) throws IOException {
        Job job = new Job(configuration,jobName);
        job.setJarByClass(HbaseTaskApp.class);

        TableMapReduceUtil.initTableMapperJob(sourceTable, scan, HBaseMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(destTable, HBaseReducer.class, job);
        job.setNumReduceTasks(1);

        return job;
    }
}
