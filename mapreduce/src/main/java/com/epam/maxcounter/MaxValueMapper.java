package com.epam.maxcounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxValueMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
  @Override
  public void map( Object key, Text value, Context context ) throws IOException, InterruptedException {
    String[] line = value.toString().split( " " );

    for ( int i = 1; i < line.length; i++ ) {
      writeValueToContext( line[ 0 ], line[ i ], context );
    }
  }

  private void writeValueToContext( String key, String value, Context context )
    throws IOException, InterruptedException {
    context.write( new IntWritable( Integer.valueOf( key ) ), new IntWritable( Integer.valueOf( value ) ) );
  }
}
