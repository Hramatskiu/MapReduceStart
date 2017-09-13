package com.epam.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

public class DriverConnectionHandler {
  public static Connection getConnection( String url, Properties info ) throws IOException {
    try {
      return DriverManager.getConnection( url, info );
    } catch ( SQLException e ) {
      throw new IOException( e );
    }
  }

  public static void registerDriver( String driverName ) throws IOException {
    try {
      Class.forName( driverName );
    } catch ( ClassNotFoundException e ) {
      throw new IOException( e );
    }
  }

  public static void deregisterAllDrivers() throws IOException {
    try {
      Enumeration<Driver> drivers = DriverManager.getDrivers();
      while ( drivers.hasMoreElements() ) {
        Driver driver = drivers.nextElement();
        DriverManager.deregisterDriver( driver );
      }
    } catch ( SQLException e ) {
      throw new IOException( e );
    }
  }

}
