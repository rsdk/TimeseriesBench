/**
 * SAP HANA write performance
 * for IoT Sensordata
 */

import java.sql.*;
import java.time.LocalDateTime;

public class perftest {
    public static void main(String[] argv) {
        Connection connection;

        final String myname = "KK_STUDENT_030";
        final String mysecret = "M7YaGrq4D3W6aG";
        final String host = "ucchana11.informatik.tu-muenchen.de";
        final String port = "31115";
        final String options = "/?autocommit=false";
        final String connstr = "jdbc:sap://" + host + ":" + port + options;
        System.out.println("Connection String: " + connstr);


        final int iValues = 2; //100000;
        final int iSensors = 20;
        final int VALUE_TYPE_TEMP_CENTIGRADE = 42;
        String tableName = "test";

        double value;
        int sensor_id;


        try {
            connection = DriverManager.getConnection(connstr, myname, mysecret);
        } catch (SQLException e) {
            System.err.println("Connection Failed. User/Passwd Error? Error: " + e);
            return;
        }
        if (connection != null) {
            try {
                System.out.println("Connection to HANA successful!");
                Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery("Select 'hello world' from dummy");
                resultSet.next();
                String hello = resultSet.getString(1);
                System.out.println(hello);

                for (int i = 0; i < iValues; i++) {
                    //LocalDateTime dt = LocalDateTime.now();
                    for (int j = 0; j < iSensors; j++) {
                        sensor_id = j;
                        value = i + j;
                        LocalDateTime dt = LocalDateTime.now();
                        System.out.print(sensor_id);
                        System.out.print(' ');
                        System.out.print(dt);
                        System.out.print(' ');
                        System.out.println(value);
                    }
                }
            } catch (SQLException e) {
                System.err.println("Query failed!");
            }
        }
    }
}
