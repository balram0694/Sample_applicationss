package com.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class JdbcM2M {
    public static void main(String[] args) throws Exception {
        String driverClass = "com.databricks.client.jdbc.Driver";
        String jdbcUrl = "jdbc:databricks://adb-2984473892726721.1.azuredatabricks.net:443";

        Properties properties = new Properties();
        properties.put("httpPath","sql/protocolv1/o/2984473892726721/1106-091846-c76zlt5z");
        properties.put("OAuth2ClientID","e8391000-426d-4cf9-b9da-6eba1e814499");
        properties.put("OAuth2Secret","dosef1f4d3d3568fb72558bb723a7eccd0c1");
        properties.put("AuthMech", "11");
        properties.put("Auth_Flow", "1");

        // SQL query
        String query = "SELECT * FROM snapindiadevteam.syed";

        // Load Databricks JDBC driver
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            System.err.println("Databricks JDBC driver not found.");
            e.printStackTrace();
            return;
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl,properties)) {
            int tmpCount=0;
            LocalDateTime currentDateTime = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            System.out.println("Start Date and Time: " + currentDateTime.format(formatter));
            for (int i = 0; i < 12; i++) {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery(query)) {
                        // Process the result set
                        while (resultSet.next()) {
                            System.out.println("Column1: " + resultSet.getString(1));
                            System.out.println("Column2: " + resultSet.getString(2));
                        }
                    }
                }
            }
            LocalDateTime currentDateTime1 = LocalDateTime.now();

            // Format the date and time
            DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            // Print the formatted date and time
            System.out.println("End Date and Time: " + currentDateTime1.format(formatter1));
            } catch(Exception e){
                e.printStackTrace();
        }
    }
}

