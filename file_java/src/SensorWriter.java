import java.io.*;
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
    private String path;
    private Writer writer;

    public SensorWriter(String path, int sensorid, int n_values, int buffSize) {
        this.path = path;
        this.sid = sensorid;
        this.n_values = n_values;
        this.buffSize = buffSize;
        try {
            // Buffered Writer vs unbuffered Writer -> unfairer Vergleich zur DB mit buffered Writer -> über flush wieder besser
            this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path + this.sid + ".csv")));
        } catch (IOException e) {
            System.err.println("Create File Failed at Sensor " + this.sid + ". Error: " + e);
        }
    }

    private String insert() {
        //StringBuffer row = new StringBuffer(60);
        LocalDateTime ldt = LocalDateTime.now();
        double value = rand.nextDouble();
        String row = this.sid + ";" + ldt + ";" + value + "\n";
        // String Buffer seems to be a little bit faster than concatenation
//        row.append(this.sid);
//        row.append(";");
//        row.append(ldt);
//        row.append(";");
//        row.append(value);
//        row.append("\n");
        return row;
    }

    public void run() {
        //StringBuffer inserts = new StringBuffer(buffSize*60);
        for (int i = 0; i < this.n_values; i++) {
            //inserts.append(this.insert());
            try {
                this.writer.write(this.insert());
            } catch (IOException e) {
                System.err.println("Write failed at Sensor " + this.sid + ". Error: " + e);
            }

            if (i % this.buffSize == 0) {
                try {
                    //this.writer.write(inserts.toString());
                    this.writer.flush();
                    //inserts = new StringBuffer(buffSize*60);
                } catch (IOException e) {
                    System.err.println("Flush failed at Sensor " + this.sid + ". Error: " + e);
                }
            }
        }
        try {
            //this.writer.write(inserts.toString());
            this.writer.close();
        } catch (IOException e) {
            System.err.println("Last commit or close failed at Sensor " + this.sid + ". Error: " + e);
        }
    }
}