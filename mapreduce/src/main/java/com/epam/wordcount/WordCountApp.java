package com.epam.wordcount;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class WordCountApp extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new WordCountApp(), args);
        System.exit(res);
    }

    private static Job createJob(JobConf configuration, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, String[] args) throws IOException{
        args = new GenericOptionsParser(configuration, args).getRemainingArgs();

        configuration.addResource("yarn-site.xml");
        configuration.addResource("hdfs-site.xml");
        configuration.addResource("core-site.xml");
        configuration.addResource("mapred-site.xml");
        configuration.addResource("ssl-client.xml");

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
            Job job = createJob(new JobConf(), WordCountMapper.class, WordCountReducer.class, strings);

            setUpInputOutputConfigs(job, "/wordcount/input/pg20417.txt", "/user/Stanislau_Hramatskiu/wordcount");

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
