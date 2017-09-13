package com.epam.hive.job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class HiveReducer extends Reducer<IntWritable, IntWritable, WritableComparable, HCatRecord> {
  @Override
  protected void reduce( IntWritable key, Iterable<IntWritable> values, Context context )
    throws IOException, InterruptedException {
    int sum =
      StreamSupport.stream( values.spliterator(), false ).map( IntWritable::get ).reduce( Integer::sum ).orElse( 0 );
    HCatRecord record = new DefaultHCatRecord( 2 );
    record.set( 0, key.get() );
    record.set( 1, sum );

    context.write( null, record );
  }
}
