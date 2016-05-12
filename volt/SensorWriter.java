import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.SplittableRandom;

/**
 * VoltDB SensorWriter
 */

class SensorWriter extends Thread {

    private final SplittableRandom rand = new SplittableRandom();
    private final int sid;
    private final int n_values;
    private final int buffSize;
    private Client client;

    public SensorWriter(String host, int sensorid, int n_values, int buffSize) {
        this.sid = sensorid;
        this.n_values = n_values;
        this.buffSize = buffSize;
        this.client = ClientFactory.createClient();

        try {
            this.client.createConnection(host);
        } catch (IOException e) {
            System.err.println("Connect to VoltDB Server Failed. Error: " + e);
            System.exit(0);
        }
    }

    private void insert() {
        LocalDateTime ldt = LocalDateTime.now();
        Timestamp sd = Timestamp.valueOf(ldt);
        double value = rand.nextDouble();
        try {
            this.client.callProcedure(new NullCallback(), "insert_sd", this.sid, sd, value);
        } catch (IOException e) {
            System.err.println("Procedure Call Failed. Error: " + e);
        }
    }

    public void run() {
        for (int i = 0; i < this.n_values; i++) {
            this.insert();
        }
        try {
            this.client.drain();
            this.client.close();
        } catch (NoConnectionsException | java.lang.InterruptedException e) {
            System.err.println("Client close failed at Sensor " + this.sid + ". Error: " + e);
        }
    }
}