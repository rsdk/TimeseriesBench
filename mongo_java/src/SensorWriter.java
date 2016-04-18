import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.sql.Timestamp;
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
    private MongoClient conn;
    private DBCollection coll;
    private DB db;
    private BasicDBObject[] batch;

    public SensorWriter(String connstr, String username, String password, int sensorid, int n_values, int buffSize) {
        this.sid = sensorid;
        this.n_values = n_values;
        this.buffSize = buffSize;
        batch = new BasicDBObject[buffSize];
        try {
            conn = new MongoClient("localhost");
            db = conn.getDB("mydb");
            coll = db.getCollection(String.valueOf(sid));
            //conn.setAutoCommit(false);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private BasicDBObject insert() {
        LocalDateTime ldt = LocalDateTime.now();
        Timestamp sd = Timestamp.valueOf(ldt);
        double value = rand.nextDouble();

        BasicDBObject doc = new BasicDBObject("sid", sid)
                .append("dt", sd)
                .append("value", value);
        return doc;
    }

    public void run() {
        int i_buffer = 0;
        for (int i = 0; i < this.n_values; i++) {
            batch[i_buffer++] = insert();
            if (i_buffer >= buffSize) {
                i_buffer = 0;
                coll.insert(batch);
            }
        }
    }
}