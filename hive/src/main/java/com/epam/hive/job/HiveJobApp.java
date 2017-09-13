package com.epam.hive.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;

public class HiveJobApp extends Configured implements Tool {
  public static void main( String[] args ) throws Exception {
    int exitCode = ToolRunner.run( new HiveJobApp(), args );
    System.exit( exitCode );
  }

  private static Job createJob( Configuration configuration, String dbName, String inputTableName,
                                String outputTableName ) throws IOException {
    Job job = new Job( configuration, "HiveJobTest" );
    HCatInputFormat.setInput( job, dbName, inputTableName );
    // initialize HCatOutputFormat

    job.setInputFormatClass( HCatInputFormat.class );
    job.setJarByClass( HiveJobApp.class );
    job.setMapperClass( HiveMapper.class );
    job.setReducerClass( HiveReducer.class );
    job.setMapOutputKeyClass( IntWritable.class );
    job.setMapOutputValueClass( IntWritable.class );
    job.setOutputKeyClass( WritableComparable.class );
    job.setOutputValueClass( DefaultHCatRecord.class );
    HCatOutputFormat.setOutput( job, OutputJobInfo.create( dbName,
      outputTableName, null ) );
    HCatSchema s = HCatOutputFormat.getTableSchema( configuration );
    HCatOutputFormat.setSchema( job, s );
    job.setOutputFormatClass( HCatOutputFormat.class );

    return job;
  }

  private static Configuration createConfiguration() {
    Configuration configuration = new Configuration();
    configuration.addResource( "core-site.xml" );
    configuration.addResource( "hive-site.xml" );
    configuration.addResource( "yarn-site.xml" );

    return configuration;
  }

  @Override
  public int run( String[] strings ) throws Exception {
    Job job = createJob( createConfiguration(), "default", "test_fortest", "test_test" );

    return job.waitForCompletion( true ) ? 1 : 0;
  }
}
