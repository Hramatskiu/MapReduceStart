package com.epam.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class HBaseMapper extends TableMapper<Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text text;

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String valueFromRow = new String(value.value());
        text.set(valueFromRow);

        context.write(text, one);
    }
}
