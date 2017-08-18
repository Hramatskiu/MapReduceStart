package com.epam.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.data.transfer.impl.HCatInputFormatReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class HiveTaskApp {
    public static void main(String[] args) throws Exception{
        ConnectionHandler connectionHandler = createConnectionHandler();

        readWithHCat();
        /*connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, QueryForTestStatements.DROP_TABLE));
        connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, QueryForTestStatements.CREATE_PARTITION_TABLE));*/
        //connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, QueryForTestStatements.CREATE_NON_PARTITION_TABLE));
        connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, HiveQueryHelper.createInsertQuery("test_fortest",
                getInsertData("2", "10122017", "someurl", "Belarus"))));
        connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, HiveQueryHelper.createInsertQuery("test_fortest",
                getInsertData("3", "15122017", "someurlj", "Poland"))));
        connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, HiveQueryHelper.createInsertQuery("test_fortest",
                getInsertData("4", "17122017", "someurlt", "Belarus"))));
        connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, QueryForTestStatements.INSERT_INTO_ANOTHER_TABLE));
        // connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, QueryForTestStatements.INSERT_INTO_LOCAL_FILE));
        /*
        connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, QueryForTestStatements.INSERT_DATA_PARTITION));*/

        DriverConnectionHandler.deregisterAllDrivers();
    }

    private static ConnectionHandler createConnectionHandler() throws IOException{
        Properties properties = new Properties();
        properties.setProperty("user", "Stanislau_Hramatskiu");
        properties.setProperty("password", "");
        //properties.loadFromXML(new FileInputStream("src/main/resources/hive-site.xml"));
        DriverConnectionHandler.registerDriver("org.apache.hive.jdbc.HiveDriver");
        return new ConnectionHandler("jdbc:hive2://svqxbdcn6hdp25n2.pentahoqa.com:10000/default?hive.exec.dynamic.partition.mode=nonstrict", properties);
    }

    private static String[] getInsertData(String id, String viewTime, String url, String country){
        return new String[]{
                id, viewTime, url, country
        };
    }

    private static void readWithHCat() throws HCatException {
        Map<String, String> clusterConfig = new HashMap<>();
        clusterConfig.put("fs.defaultFs", "hdfs://svqxbdcn6hdp25n1.pentahoqa.com:8020");
        clusterConfig.put("hive.metastore.warehouse.dir", "/apps/hive/warehouse");
        ReadEntity.Builder builder = new ReadEntity.Builder();
        ReadEntity readEntity = builder.withDatabase("default").withTable("test_fortest").build();
        HCatReader reader = DataTransferFactory.getHCatReader(readEntity, clusterConfig);

        ReaderContext readerContext = reader.prepareRead();
        HCatReader additionalReader = new HCatInputFormatReader(readerContext, 0, null);
        Iterator<HCatRecord> recordIterator = additionalReader.read();

        while (recordIterator.hasNext()){
            HCatRecord hCatRecord = recordIterator.next();
            System.out.println(hCatRecord.get(1));
        }
        /*for(InputSplit split : readerContext.getSplits()){
            HCatReader hCatReader = DataTransferFactory.getHCatReader(split,
                    readerContext.getConf());
            Iterator<HCatRecord> itr = reader.read();
            while(itr.hasNext()){
                HCatRecord read = itr.next();
            }
        }*/
    }

    private static Configuration createConfiguration(){
        Configuration configuration = new Configuration();
        configuration.addResource("hive-site.xml");
        configuration.addResource("yarn-site.xml");

        return configuration;
    }
}
