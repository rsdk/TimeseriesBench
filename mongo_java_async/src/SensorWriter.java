import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.bson.Document;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

//import com.mongodb.async.SingleResultCallback;

class SensorWriter extends Thread {

    private final Random rand = new Random();
    private final int sid;
    private final int n_values;
    private final int buffSize;
    private final MongoClient mClient;
    private final MongoCollection coll;
    private final ArrayList<Document> batch;
    private AtomicInteger countert;


    public SensorWriter(MongoClient connstr, int sensorid, int n_values, int buffSize) {
        this.sid = sensorid;
        this.n_values = n_values;
        this.buffSize = buffSize;
        this.mClient = connstr;
        batch = new ArrayList<>(buffSize);
        //mClient = MongoClients.create(connstr);
        MongoDatabase db = mClient.getDatabase("mydb");
        coll = db.getCollection(String.valueOf("test" + sid));
    }

    private Document insert() {
        long sd = Timestamp.valueOf(LocalDateTime.now()).getTime();
        double value = rand.nextDouble();

        return new Document("sid", sid)
                .append("dt", sd)
                .append("value", value);
    }

    public void run() {
        countert = new AtomicInteger();
        for (int i = 0; i < this.n_values; i++) {
            batch.add(insert());
            if (i % buffSize == 0) {
                if (countert.get() > 1000) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        System.out.print("too many threads: waiting " + e);
                    }
                } else if (countert.get() > 10000) {
                    try {
                        System.out.print("too many threads: waiting");
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        System.out.print("way too many threads: waiting " + e);
                    }
                }
                countert.getAndIncrement();
                coll.insertMany(batch, new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        countert.getAndDecrement();
                        if (t != null) {
                            System.out.println("Callback: " + t.toString());
                        }
                    }
                });
                batch.clear();
            }
        }
        if (batch.size() > 0) {
            countert.getAndIncrement();
            coll.insertMany(batch, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    countert.getAndDecrement();
                    if (t != null) {
                        System.out.println(t.toString());
                    }
                }
            });
        }
        while (countert.get() > 0) {
            //System.err.print(countert.get() + " ");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                System.out.print("wait failed at sensorwriter" + e);
            }

        }
        batch.clear();
        //mClient.close();
    }
}