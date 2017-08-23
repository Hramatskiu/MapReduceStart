package com.epam.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class WordCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
    private String spaceRegex = "\\s+";
    private String punctRegex = "\\p{Punct}";
    private Text word = new Text();

    @Override
    public void map(Object o, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String[] separatedWords = text.toString().trim().toUpperCase().replaceAll(punctRegex, " ").split(spaceRegex);

        for (String separatedWord : separatedWords) {
            saveValueToCollector(outputCollector, separatedWord);
            reporter.progress();
        }
    }

    private void saveValueToCollector(OutputCollector<Text, IntWritable> outputCollector, String value) throws IOException{
        word.set(value);
        outputCollector.collect(word, new IntWritable(1));
    }

}
