import java.sql.*;
import java.time.LocalDateTime;
import java.util.Random;

/**
 * Created by rene on 09.04.2016.
 */

class SensorWriter extends Thread {

    private final Random rand = new Random();
    private final int sid;
    private final int n_values;
    private final int buffSize;
    private Connection conn;
    private PreparedStatement pstmt;

    public SensorWriter(String connstr, String username, String password, int sensorid, int n_values, int buffSize) {
        this.sid = sensorid;
        this.n_values = n_values;
        this.buffSize = buffSize;
        try {
            this.conn = DriverManager.getConnection(connstr, username, password);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Connection Failed. Error: " + e);
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
            System.exit(1);
        }
        try {
            pstmt = conn.prepareStatement("INSERT INTO test VALUES(?,?,?)");

        } catch (SQLException e) {
            System.err.println("Prepare Statement Failed. Error: " + e);
        }
    }

    private void insert() {
        LocalDateTime ldt = LocalDateTime.now();
        Timestamp sd = Timestamp.valueOf(ldt);
        double value = rand.nextDouble();
        try {
            pstmt.setInt(1, sid);
            pstmt.setTimestamp(2, sd);
            pstmt.setDouble(3, value);
            pstmt.addBatch();
        } catch (SQLException e) {
            System.err.println("Execute Update Failed at Sensor " + sid + ". Error: " + e);
        }
    }

    public void run() {
        for (int i = 0; i < n_values; i++) {
            this.insert();

            if (i % buffSize == 0) {
                try {
                    pstmt.executeBatch();
                    pstmt.clearWarnings();
                    conn.commit();
                } catch (SQLException e) {
                    System.err.println("Commit failed at Sensor " + sid + ". Error: " + e);
                }
            }
        }
        try {
            pstmt.executeBatch();
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            System.err.println("Last commit or close failed at Sensor " + sid + ". Error: " + e);
        }
    }
}