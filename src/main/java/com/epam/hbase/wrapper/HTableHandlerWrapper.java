package com.epam.hbase.wrapper;

import com.epam.hbase.HBaseConnectionPool;
import com.epam.hbase.HBaseConnectionWrapper;
import com.epam.hbase.HBaseQueryHelper;
import com.epam.hbase.HBaseUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;

public class HTableHandlerWrapper {
    private HBaseConnectionPool hBaseConnectionPool;

    public HTableHandlerWrapper(HBaseConnectionPool hBaseConnectionPool){
        this.hBaseConnectionPool = hBaseConnectionPool;
    }

    public List<String> getTableFamilies(String tableName){
        try(HBaseConnectionWrapper hBaseConnectionWrapper = new HBaseConnectionWrapper(hBaseConnectionPool)) {
            return Arrays.stream(getTable(hBaseConnectionWrapper, tableName).getTableDescriptor().getColumnFamilies()).map(HColumnDescriptor::getNameAsString).collect(Collectors.toList());
        }
        catch (IOException ex){
            //logging
        }

        return new ArrayList<>();
    }

    public boolean createTable(String tableName, String familyName) {
        try(HBaseConnectionWrapper hBaseConnectionWrapper = new HBaseConnectionWrapper(hBaseConnectionPool)) {
            return createTable(hBaseConnectionWrapper, tableName, familyName);
        }
        catch (IOException ex){
            //logging
        }

        return false;
    }

    public NavigableMap<byte[], byte[]> getFamilyMap(String tableName, String rowName, String familyName) {
        try(HBaseConnectionWrapper hBaseConnectionWrapper = new HBaseConnectionWrapper(hBaseConnectionPool)) {
            return getFamilyMap(getTable(hBaseConnectionWrapper, tableName).get(HBaseQueryHelper.createGetSentence(rowName)), familyName);
        }
        catch (IOException ex){
            //logging
        }

        return null;
    }

    public void addFamilyToTable(String tableName, String familyName){
        try(HBaseConnectionWrapper hBaseConnectionWrapper = new HBaseConnectionWrapper(hBaseConnectionPool)) {
            hBaseConnectionWrapper.getAdmin().addColumn(TableName.valueOf(tableName), new HColumnDescriptor(Bytes.toBytes(familyName)));
            //getTable(hBaseConnectionWrapper, tableName).getTableDescriptor().addFamily(new HColumnDescriptor(Bytes.toBytes(familyName)));
        }
        catch (IOException ex){
            //logging
        }
    }


    public List<String> getTableNamesList(){
        try(HBaseConnectionWrapper hBaseConnectionWrapper = new HBaseConnectionWrapper(hBaseConnectionPool)) {
            return Arrays.stream(hBaseConnectionWrapper.getAdmin().listTableNames()).map(TableName::toString).collect(Collectors.toList());
        }
        catch (IOException ex){
            //logging
        }

        return new ArrayList<>();
    }

    public void putDataInTable(String tableName, String rowName, String familyName, String qualifier, String value){
        try(HBaseConnectionWrapper hBaseConnectionWrapper = new HBaseConnectionWrapper(hBaseConnectionPool)) {
            HBaseQueryHelper.tryToPutData(rowName, familyName, qualifier, value, getTable(hBaseConnectionWrapper, tableName));
        }
        catch (IOException ex){
            //logging
        }
    }

    public ResultScanner getResultScanner(String tableName, String familyName, String qualifierName){
        try(HBaseConnectionWrapper hBaseConnectionWrapper = new HBaseConnectionWrapper(hBaseConnectionPool)) {
            return getTable(hBaseConnectionWrapper, tableName).getScanner(HBaseQueryHelper.createScanForColumn(familyName, qualifierName));
        }
        catch (IOException ex){
            //logging
        }

        return null;
    }

    public void dropTable(String tableName){
        try(HBaseConnectionWrapper hBaseConnectionWrapper = new HBaseConnectionWrapper(hBaseConnectionPool)) {
            hBaseConnectionWrapper.getAdmin().deleteTable(TableName.valueOf(tableName));
        }
        catch (IOException ex){
            //logging
        }
    }

    private NavigableMap<byte[], byte[]> getFamilyMap(Result result, String familyName){
        return result.getFamilyMap(Bytes.toBytes(familyName));
    }

    private Table getTable(HBaseConnectionWrapper connection, String tableName) throws IOException{
        return connection.getTable(TableName.valueOf(tableName));
    }

    private boolean createTable(HBaseConnectionWrapper connection, String tableName, String familyName) throws IOException{
        List<String> familyNames = new ArrayList<>();
        familyNames.add(familyName);

        return createTable(connection, tableName, familyNames);
    }

    private boolean createTable(HBaseConnectionWrapper connection, String tableName, List<String> familyNames) throws IOException{
        Admin admin = connection.getAdmin();

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        familyNames.forEach(familyName -> tableDescriptor.addFamily(new HColumnDescriptor(familyName)));

        if (!admin.tableExists(tableDescriptor.getTableName())){
            admin.createTable(tableDescriptor);
            return true;
        }

        return false;
    }
}
