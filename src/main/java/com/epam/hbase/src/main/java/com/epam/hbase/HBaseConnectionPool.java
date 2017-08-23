package com.epam.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HBaseConnectionPool implements Closeable{
    private Set<Connection> availableConnections;
    private Set<Connection> connectionsInUse;

    public HBaseConnectionPool(int conCount, Configuration configuration)  {
        availableConnections = new HashSet<>();
        connectionsInUse = new HashSet<>();
        try {
            for (int i = 0; i < conCount; i++){
                availableConnections.add(ConnectionFactory.createConnection(configuration));
            }
        }
        catch (IOException ex){
            //logging
        }

    }

    public synchronized Connection takeConnection(){
        Connection connection = availableConnections.iterator().next();
        availableConnections.remove(connection);
        connectionsInUse.add(connection);

        return connection;
    }

    public synchronized void releaseConnection(Connection connection){
        if (connectionsInUse.contains(connection)){
            connectionsInUse.remove(connection);
            availableConnections.add(connection);
        }
    }

    @Override
    public void close() throws IOException {
        closeAndClearConnectionsSets(connectionsInUse);
        closeAndClearConnectionsSets(availableConnections);
    }

    private void closeAndClearConnectionsSets(Set<Connection> connections) throws IOException{
        for (Connection connection:connections){
            connection.close();
        }

        connections.clear();
    }
}
