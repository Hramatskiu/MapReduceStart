package com.epam.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Function;

public class ConnectionHandler {
  private String url;
  private Properties info;

  public ConnectionHandler( String url, Properties info ) {
    this.info = info;
    this.url = url;
  }

  public <T> T performTaskOnConnection( Function<Connection, T> function ) {
    try ( Connection connection = DriverConnectionHandler.getConnection( url, info ) ) {
      return function.apply( connection );
    } catch ( IOException | SQLException ex ) {
      System.out.println( ex.getMessage() );
    }

    return null;
  }
}
