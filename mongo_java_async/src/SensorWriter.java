import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.bson.Document;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Random;

//import com.mongodb.async.SingleResultCallback;

class SensorWriter extends Thread {

    private final Random rand = new Random();
    private final int sid;
    private final int n_values;
    private final int buffSize;
    private final MongoClient mClient;
    private final MongoCollection coll;
    private final ArrayList<Document> batch;


    public SensorWriter(MongoClient connstr, int sensorid, int n_values, int buffSize) {
        this.sid = sensorid;
        this.n_values = n_values;
        this.buffSize = buffSize;
        this.mClient = connstr;
        batch = new ArrayList<>(buffSize);
        //mClient = MongoClients.create(connstr);
        MongoDatabase db = mClient.getDatabase("mydb");
        coll = db.getCollection(String.valueOf(sid));
    }

    private Document insert() {
        long sd = Timestamp.valueOf(LocalDateTime.now()).getTime();
        double value = rand.nextDouble();

        return new Document("sid", sid)
                .append("dt", sd)
                .append("value", value);
    }

    public void run() {

        for (int i = 0; i < this.n_values; i++) {
            batch.add(insert());
            if (i % buffSize == 0) {
                coll.insertMany(batch, (aVoid, throwable) -> {
                    //System.out.print(throwable);
                }); // no Callback necessary
                batch.clear();
            }
        }
        //mClient.close();
    }
}