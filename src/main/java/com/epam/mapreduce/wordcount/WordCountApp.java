package com.epam.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.stream.StreamSupport;

public class WordCountApp extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new WordCountApp(), args);
        System.exit(res);
    }

    private static Job createJob(JobConf configuration, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass) throws IOException{
        /*configuration.set("fs.default.name", "hdfs://svqxbdcn6hdp25n1.pentahoqa.com:8020");
        configuration.set("mapred.job.tracker", "svqxbdcn6hdp25n1.pentahoqa.com:8020");*/

        configuration.set("yarn.resourcemanager.address", "svqxbdcn6hdp25n2.pentahoqa.com:8050");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("fs.defaultFS", "hdfs://svqxbdcn6hdp25n1.pentahoqa.com:8020/");
       /* configuration.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH, "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
                + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
                + "$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,"
                + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*");*/
        configuration.set("yarn.application.classpath",
                "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
                        + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
                        + "$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,"
                        + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*");
        configuration.set("yarn.app.mapreduce.am.staging-dir", "user");

        Job job = new Job(configuration);

        job.setJarByClass(WordCountApp.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }

    private static void setUpInputOutputConfigs(Job job, String inputPath, String outputPath) throws IOException{
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }

    @Override
    public int run(String[] strings) throws Exception {
        try{
            Job job = createJob(new JobConf(), WordCountMapper.class, WordCountReducer.class);

            setUpInputOutputConfigs(job, "/wordcount/input/fortest.txt", "/wordcount/output/output1.txt");

            return job.waitForCompletion(true) ? 1 : 0;
        }
        catch (ArrayIndexOutOfBoundsException | IOException | InterruptedException ex){
            //logging
            //logger.log(Level.ERROR, ex.getMessage());
            System.out.println(ex.getMessage());
        }

        return 0;
    }

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private String spaceRegex = "\\s+";
        private String punctRegex = "\\p{Punct}";
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, final Context context) throws IOException, InterruptedException {
            String[] separatedWords = value.toString().trim().toUpperCase().replaceAll(punctRegex, " ").split(spaceRegex);

            for (String separatedWord : separatedWords) {
                System.out.println(separatedWord);
                writeValueToContext(separatedWord, context);
            }
        }

        private void writeValueToContext(String value, Context context) throws IOException, InterruptedException{
            word.set(value);
            context.write(word, new IntWritable(1));
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = StreamSupport.stream(values.spliterator(), false).map(IntWritable::get).reduce(0, (a, b) -> a + b);

            context.write(key, new IntWritable(sum));
        }
    }
}
