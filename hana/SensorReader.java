import java.sql.*;

/**
 * Created by rene on 14.05.16.
 */
public class SensorReader {

    private Connection conn;

    public SensorReader(String connstr, String username, String password) {

        try {
            conn = DriverManager.getConnection(connstr, username, password);
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            System.err.println("Connection Failed. Error: " + e);
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
            System.exit(1);
        }
    }

    public double[] get_mean_for_sid(int sid) {
        PreparedStatement pstmt;
        //Timestamp sd = Timestamp.valueOf(ldt);
        double res[] = new double[4];
        try {
            pstmt = conn.prepareStatement("SELECT min(V), max(V), avg(V), stddev(V) " +
                    "FROM test WHERE SID=?");
            pstmt.setInt(1, sid);
            //pstmt.setTimestamp(2, sd);
            //pstmt.setDouble(3, value);
            ResultSet result = pstmt.executeQuery();
            if (result.next()) {
                res[0] = result.getDouble(1); //min
                res[1] = result.getDouble(2); //max
                res[2] = result.getDouble(3); //mean
                res[3] = result.getDouble(4); //stddev
            }
            result.close();
        } catch (SQLException e) {
            System.err.println("get_stats_failed: " + sid + ". Error: " + e);
        }
        return res;
    }

    public double[] get_mean_for_sid(int sid, Timestamp start, Timestamp end) {
        PreparedStatement pstmt;
        //Timestamp sd = Timestamp.valueOf(ldt);
        double res[] = new double[4];
        try {
            pstmt = conn.prepareStatement("SELECT min(V), max(V), avg(V), stddev(V) " +
                    "FROM test WHERE SID=? AND T BETWEEN ? AND ?");
            pstmt.setInt(1, sid);
            pstmt.setTimestamp(2, start);
            pstmt.setTimestamp(3, end);
            //pstmt.setDouble(3, value);
            ResultSet result = pstmt.executeQuery();
            if (result.next()) {
                res[0] = result.getDouble(1); //min
                res[1] = result.getDouble(2); //max
                res[2] = result.getDouble(3); //mean
                res[3] = result.getDouble(4); //stddev
            }
            result.close();
        } catch (SQLException e) {
            System.err.println("get_stats_failed: " + sid + ". Error: " + e);
        }
        return res;
    }

    public double[] get_mean_for_sid(int sid, long ms_before) {
        PreparedStatement pstmt;
        Timestamp latest;
        double res[] = new double[4];
        try {
            pstmt = conn.prepareStatement("SELECT max(t) FROM test WHERE sid=?");
            pstmt.setInt(1, sid);
            ResultSet result_t = pstmt.executeQuery();
            if (result_t.next()) {
                latest = result_t.getTimestamp(1);
                if (latest != null) {
                    pstmt = conn.prepareStatement("SELECT min(v), max(v), avg(v), " +
                            "stddev(v) FROM test WHERE sid=? AND d BETWEEN ? AND ?");
                    pstmt.setInt(1, sid);
                    pstmt.setTimestamp(2, new Timestamp(latest.getTime() - ms_before));
                    pstmt.setTimestamp(3, latest);
                    //pstmt.setDouble(3, value);
                    ResultSet result = pstmt.executeQuery();
                    if (result.next()) {
                        res[0] = result.getDouble(1); //min
                        res[1] = result.getDouble(2); //max
                        res[2] = result.getDouble(3); //mean
                        res[3] = result.getDouble(4); //stddev
                    }
                    result.close();
                }
            }
            result_t.close();


        } catch (SQLException e) {
            System.err.println("get_stats_failed: " + sid + ". Error: " + e);
        }
        return res;
    }

    public double get_count() {
        PreparedStatement pstmt;
        double res = 0;
        try {
            pstmt = conn.prepareStatement("SELECT count(*) FROM test");
            ResultSet result = pstmt.executeQuery();
            if (result.next()) {
                res = result.getDouble(1); //count
            }
            result.close();
        } catch (SQLException e) {
            System.err.println("get_count_failed Error: " + e);
        }
        return res;
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("error while closing db conn: " + e);
        }
    }
}