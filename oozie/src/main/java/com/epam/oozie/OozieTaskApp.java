package com.epam.oozie;

import com.epam.kerberos.HadoopKerberosUtil;
import com.epam.kerberos.KerberosUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.io.FileInputStream;
import java.util.Properties;

public class OozieTaskApp extends Configured implements Tool {
  public static void main( String[] args ) throws Exception {
    int res = ToolRunner.run( new Configuration(), new OozieTaskApp(), args );
    System.exit( res );
  }

  @Override
  public int run( String[] strings ) throws Exception {
    HadoopKerberosUtil.doLogin( "devuser@PENTAHOQA.COM", "password" );
    OozieClient wc = new OozieClient( "http://svqxbdcn6hdp26secn2.pentahoqa.com:11000/oozie" );
    AuthOozieClient oozieClient = new AuthOozieClient( "http://svqxbdcn6hdp26secn2.pentahoqa.com:11000/oozie" );
    OozieConfigLoader oozieConfigLoader = new OozieConfigLoader();
    //String version = oozieClient.getClientBuildVersion();

    Properties conf = wc.createConfiguration();
    oozieConfigLoader.loadConfigFromXml( conf, "oozie-site.xml" );

    conf.setProperty( OozieClient.APP_PATH,
      "hdfs://svqxbdcn6hdp26secn1.pentahoqa.com:8020/user/devuser/examples/examples/app/map-reduce/workflow.xml" );
    //conf.setProperty("jobTracker", "svqxbdcn6hdp26secn2.pentahoqa.com:8050");
    //conf.setProperty("nameNode", "hdfs://svqxbdcn6hdp26secn1.pentahoqa.com:8020");
    //conf.setProperty("queueName", "default");
    conf.setProperty( "airawatOozieRoot",
      "hdfs://svqxbdcn6hdp26secn1.pentahoqa.com:8020/user/devuser/examples/examples/app/map-reduce" );
    conf.setProperty( "oozie.libpath", "hdfs://svqxbdcn6hdp26secn1.pentahoqa.com:8020/user/oozie/share/lib" );
    conf.setProperty( "oozie.use.system.libpath", "true" );
    conf.setProperty( "oozie.wf.rerun.failnodes", "true" );

    String jobId = wc.run( conf );

    while ( wc.getJobInfo( jobId ).getStatus() == WorkflowJob.Status.RUNNING ) {
      System.out.println( "Workflow job running ..." );
      Thread.sleep( 10 * 1000 );
    }

    return 0;
  }
}
