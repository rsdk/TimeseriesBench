/**
 * SAP HANA write performance
 * for IoT Sensordata
 */

import java.sql.Timestamp;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.Random;

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
//                ResultSet resultSet = stmt.executeQuery("Select 'hello world' from dummy");
//                resultSet.next();
//                String hello = resultSet.getString(1);
//                System.out.println(hello);

                final long startTime = System.currentTimeMillis();
                for (int i = 0; i < iValues; i++) {
                    //LocalDateTime dt = LocalDateTime.now();
                    for (int j = 0; j < iSensors; j++) {
                        sensor_id = j;
                        value = i + j;
                        LocalDateTime dt = LocalDateTime.now();
                        /*System.out.print(sensor_id);
                        System.out.print(' ');
                        System.out.print(dt);
                        System.out.print(' ');
                        System.out.println(value);*/
                    }
                }
                final int lines = iValues * iSensors;
                final long endTime = System.currentTimeMillis();
                final double delta = (double) endTime - (double) startTime;

                System.out.printf("Runtime: %f.\n", delta);
                System.out.printf("Time per Insert: %8.8f\n", delta / lines);
                System.out.printf("Inserts per Second: %8.8f\n", 1 / (delta / lines) * 1000);
            } catch (SQLException e) {
                System.err.println("Query failed!");
            }
        }
    }

    private class SensorWriter {

        Random rand = new Random();
        private Connection conn;
        private int sid;
        private int n_values;
        private PreparedStatement pstmt;

        public SensorWriter(String connstr, String username, String password, int sensorid, int n_values) {
            this.sid = sensorid;
            this.n_values = n_values;
            try {
                this.conn = DriverManager.getConnection(connstr, username, password);
            } catch (SQLException e) {
                System.err.println("Connection Failed. Error: " + e);
                return;
            }
            try {
                this.pstmt = this.conn.prepareStatement("INSERT INTO test VALUES(?,?,?)");
                this.pstmt.setInt(1, this.sid);
            } catch (SQLException e) {
                System.err.println("Prepare Statement Failed. Error: " + e);
            }
        }

        private void insert() {
            LocalDateTime ldt = LocalDateTime.now();
            Timestamp sd = Timestamp.valueOf(ldt);
            double value = rand.nextDouble();
            try {
                this.pstmt.setTimestamp(2, sd);
                this.pstmt.setDouble(3, value);
                this.pstmt.executeUpdate();
            } catch (SQLException e) {
                System.err.println("Execute Update Failed at Sensor " + this.sid + ". Error: " + e);
            }
        }

        public void run() {
            for (int i = 0; i < this.n_values; i++) {
                this.insert();
            }
        }
    }



}
