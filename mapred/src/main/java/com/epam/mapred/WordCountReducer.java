package com.epam.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
  @Override
  public void reduce( Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector,
                      Reporter reporter ) throws IOException {
    int sum = StreamSupport.stream( Spliterators.spliteratorUnknownSize( iterator, 0 ), false ).map( IntWritable::get )
      .reduce( 0, ( a, b ) -> a + b );

    outputCollector.collect( text, new IntWritable( sum ) );
  }
}
