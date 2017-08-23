package com.epam.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class HBaseConnectionWrapper implements Closeable{
    private Connection connection;
    private HBaseConnectionPool hBaseConnectionPool;

    public HBaseConnectionWrapper(HBaseConnectionPool hBaseConnectionPool){
        this.connection = hBaseConnectionPool.takeConnection();
        this.hBaseConnectionPool = hBaseConnectionPool;
    }

    public Configuration getConfiguration() {
        return connection.getConfiguration();
    }

    public Table getTable(TableName tableName) throws IOException {
        return connection.getTable(tableName);
    }

    public Table getTable(TableName tableName, ExecutorService executorService) throws IOException {
        return connection.getTable(tableName, executorService);
    }

    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return connection.getBufferedMutator(tableName);
    }

    public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams) throws IOException {
        return connection.getBufferedMutator(bufferedMutatorParams);
    }

    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return connection.getRegionLocator(tableName);
    }

    public Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    public void close() throws IOException {
        hBaseConnectionPool.releaseConnection(connection);
    }

    public boolean isClosed() {
        return connection.isClosed();
    }

    public void abort(String s, Throwable throwable) {
        connection.abort(s, throwable);
    }

    public boolean isAborted() {
        return connection.isAborted();
    }
}
