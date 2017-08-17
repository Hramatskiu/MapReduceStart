package com.epam.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveQueryHelper {
    public static ResultSet executeQuery(Connection connection, String query){
        try(Statement statement = connection.createStatement()) {
            return statement.executeQuery(query);
        }
        catch (SQLException ex){
            //logging
            System.out.println(ex.getMessage());
        }

        return null;
    }

}
