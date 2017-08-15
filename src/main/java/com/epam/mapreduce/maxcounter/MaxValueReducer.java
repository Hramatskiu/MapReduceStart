package com.epam.mapreduce.maxcounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class MaxValueReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = StreamSupport.stream(values.spliterator(), false).map(IntWritable::get).max(Integer::compare).orElse(0);

        context.write(key, new IntWritable(max));
    }
}
