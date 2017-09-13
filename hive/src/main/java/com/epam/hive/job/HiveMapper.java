package com.epam.hive.job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;

public class HiveMapper extends Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {
  @Override
  protected void map( WritableComparable key, HCatRecord value, Context context )
    throws IOException, InterruptedException {
    context.write( new IntWritable( (Integer) value.get( 1 ) ), new IntWritable( 1 ) );
  }
}
