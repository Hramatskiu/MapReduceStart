package com.epam.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    public static String createInsertQuery(String tableName, String[] valuesList){
        String values = "(" + Arrays.stream(valuesList).map(value -> "'" + value + "'").collect(Collectors.joining(",")) + ")";

        return QueryForTestStatements.INSERT_FIRST_PART_STATEMENT + " " + tableName + " " + QueryForTestStatements.INSERT_VALUES_PART_STATEMENT + " " + values;
    }
}
