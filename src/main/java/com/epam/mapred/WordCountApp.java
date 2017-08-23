package com.epam.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCountApp extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new com.epam.mapreduce.src.main.java.com.epam.wordcount.WordCountApp(), args);
        System.exit(res);
    }

    private static JobConf createJob(JobConf configuration, Class<? extends org.apache.hadoop.mapred.Mapper> mapperClass, Class<? extends org.apache.hadoop.mapred.Reducer> reducerClass, String[] args) throws IOException {
        args = new GenericOptionsParser(configuration, args).getRemainingArgs();

        configuration.addResource("yarn-site.xml");
        configuration.addResource("hdfs-site.xml");

        JobConf job = new JobConf(configuration);

        job.setJarByClass(com.epam.mapreduce.src.main.java.com.epam.wordcount.WordCountApp.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        return job;
    }

    private static void setUpInputOutputConfigs(JobConf job, String inputPath, String outputPath) throws IOException{
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }

    @Override
    public int run(String[] strings) throws Exception {
        try{
            JobConf job = createJob(new JobConf(), WordCountMapper.class, WordCountReducer.class, strings);

            setUpInputOutputConfigs(job, "/wordcount/input/fortest.txt", "/wordcount/output/output2");
            JobClient.runJob(job);

            return 1;
        }
        catch (ArrayIndexOutOfBoundsException | IOException ex){
            //logging
            //logger.log(Level.ERROR, ex.getMessage());
            System.out.println(ex.getMessage());
        }

        return 0;
    }
}
