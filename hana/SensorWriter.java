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
            if (i % this.buffSize == 0) {
                try {
                    this.conn.commit();
                } catch (SQLException e) {
                    System.err.println("Commit failed at Sensor " + this.sid + ". Error: " + e);
                }
            }
        }
        try {
            this.conn.commit();
            this.conn.close();
        } catch (SQLException e) {
            System.err.println("Last commit or close failed at Sensor " + this.sid + ". Error: " + e);
        }
    }
}