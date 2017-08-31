package com.epam.sqoop;

import com.epam.sqoop.creator.SqoopConfigsCreator;
import com.epam.sqoop.creator.SqoopJobCreator;
import com.epam.sqoop.creator.SqoopLinkCreator;
import com.epam.sqoop.util.SqoopUtil;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.validation.Status;

public class SqoopTaskApp {
    private static final String SQOOP_URL_STRING = "http://svqxbdcn6hdp25n1.pentahoqa.com:12000/sqoop";

    public static void main(String[] args) throws Exception{
        //HadoopKerberosUtil.doLogin("devuser@PENTAHOQA.COM", "password");
        SqoopClient sqoopClient = new SqoopClient(SQOOP_URL_STRING);
        testConnection(sqoopClient);
        createAndSaveLinks(sqoopClient);
        readLinksValidationMessages(sqoopClient);
        createAndSaveJob(sqoopClient);
    }

    private static void readLinksValidationMessages(SqoopClient sqoopClient){
        sqoopClient.getLinks().stream().map(MLink::getConnectorLinkConfig).map(MConfigList::getConfigs).forEach(SqoopUtil::printValidationMessages);
    }

    private static void testConnection(SqoopClient sqoopClient){
        SqoopUtil.describe(sqoopClient.getConnector("hdfs-connector").getLinkConfig().getConfigs(), sqoopClient.getConnectorConfigBundle("hdfs-connector"));
        SqoopUtil.describe(sqoopClient.getConnector("hive-connector").getLinkConfig().getConfigs(), sqoopClient.getConnectorConfigBundle("hive-connector"));
        SqoopUtil.describe(sqoopClient.getConnector("mysql-connector").getLinkConfig().getConfigs(), sqoopClient.getConnectorConfigBundle("mysql-connector"));
    }

    private static void createAndSaveLinks(SqoopClient sqoopClient){
        saveLinkWithLog(sqoopClient, createFromLink(sqoopClient));
        saveLinkWithLog(sqoopClient, createToLink(sqoopClient));
    }

    private static void createAndSaveJob(SqoopClient sqoopClient){
        saveJobWithLog(sqoopClient, createJob(sqoopClient));
    }

    private static void saveLinkWithLog(SqoopClient sqoopClient, MLink link){
        Status status = sqoopClient.saveLink(link);
        if(status.canProceed()) {
            System.out.println("Created Link with Link Id : " + link.getPersistenceId());
        } else {
            System.out.println("Something went wrong creating the link");
        }
    }

    private static void saveJobWithLog(SqoopClient sqoopClient, MJob job){
        Status status = sqoopClient.saveJob(job);
        if(status.canProceed()) {
            System.out.println("Created Job with Job Name: "+ job.getName());
        } else {
            System.out.println("Something went wrong creating the job");
        }
    }

    private static MLink createFromLink(SqoopClient sqoopClient){
        return SqoopLinkCreator.createLink(sqoopClient, "hive-connector", "fromLink", "devuser",
                SqoopConfigsCreator.createConfigMapForJDBCConnector("jdbc:hive2://svqxbdcn6hdp25n2.pentahoqa.com:10000/default?hive.exec.dynamic.partition.mode=nonstrict",
                        "org.apache.hive.jdbc.HiveDriver", "root", "password"));
    }

    private static MLink createToLink(SqoopClient sqoopClient){
        return SqoopLinkCreator.createLink(sqoopClient, "hdfs-connector", "toLink", "devuser",
                SqoopConfigsCreator.createConfigMapForHDFSConnector("hdfs://svqxbdcn6hdp25n1.pentahoqa.com:8020"));
    }

    private static MJob createJob(SqoopClient sqoopClient){
        return SqoopJobCreator.createJob(sqoopClient, "fromLink", "toLink", "test_job", "devuser",
                SqoopConfigsCreator.createJobFromTableConfig("default", "fortest", "id"),
                SqoopConfigsCreator.createJobOtHDFSConfig("/user/Stanislau_Hramatskiu/sqoop_test"), SqoopConfigsCreator.createJobDriverConfig(3));
    }
}
