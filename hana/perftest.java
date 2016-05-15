import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 * SAP HANA write performance
 * for IoT Sensordata
 */


public class perftest {
    public static void main(String[] argv) {

        final String myname = "KK_MASTER";//"SYSTEM";
        final String mysecret = "ZvLfSDG9DWvVrt"; //"benchAT16";
        final String host = "ucchana11.informatik.tu-muenchen.de"; //"10.0.13.183"; //
        final String port = "31115"; //"30015";
        final String options = "/?autocommit=false&reconnect=true";
        final String connstr = "jdbc:sap://" + host + ":" + port + options;
        System.out.println("Connection String: " + connstr);

        final int iValues;
        final int iSensors;
        final int buffSize;
        if (argv.length < 3) {
            iValues = 100;
            iSensors = 1;
            buffSize = 10;
        } else {
            iValues = Integer.parseInt(argv[0]); // Values pro Sensor
            iSensors = Integer.parseInt(argv[1]);
            buffSize = Integer.parseInt(argv[2]);
        }

        String tableName = "test";

        final long startTime = System.currentTimeMillis();
        SensorWriter[] s_threads = new SensorWriter[iSensors];

        // Start a thread for every Sensor
        for (int i = 0; i < iSensors; i++) {
            s_threads[i] = new SensorWriter(connstr, myname, mysecret, i, iValues, buffSize);
            s_threads[i].start();
        }

        // Wait till all Threads are finished
        outerloop:
        while (true) {
            int alive_count = iSensors;
            for (int i = 0; i < iSensors; i++) {
                if (s_threads[i].isAlive()) {
                    try {
                        Thread.sleep(100);
                        continue outerloop;
                    } catch (InterruptedException e) {
                        System.err.println("Error while waiting for Threads to finish. Error: " + e);
                    }
                } else if (!(s_threads[i].isAlive()) && alive_count <= 1) {
                    break outerloop;
                } else {
                    alive_count--;
                }
            }
        }

        final int lines = iValues * iSensors;
        final long endTime = System.currentTimeMillis();
        final double delta = (double) endTime - (double) startTime;

        System.out.printf("Runtime: %f.\n", delta);
        System.out.printf("Time per Insert: %.3f ms\n", delta / lines);
        System.out.printf("Inserts per Second: %.0f\n", 1 / (delta / lines) * 1000);
        try (PrintWriter out = new PrintWriter("results.txt")) {
            out.println("hana;" + delta + ";" + lines + ";" + iValues + ";" + iSensors + ";" + buffSize);
        } catch (FileNotFoundException e) {
            System.out.println("Error while writing results: " + e);
        }
        SensorReader sr = new SensorReader(connstr, myname, mysecret);
        double count = sr.get_count();

        System.out.printf("Count: %.0f", count);
        System.out.println();

        final long startTime_read = System.currentTimeMillis();
        double[] res = sr.get_mean_for_sid(0);
        final long endTime_read = System.currentTimeMillis();
        final double delta_read = (double) endTime_read - (double) startTime_read;
        System.out.printf("Time for read min/max/mean/stddev: %.0f ms\n", delta_read);
        System.out.println();

        System.out.println("min, max, mean and stddev: ");
        for (int i = 0; i < res.length; i++) {
            System.out.println(res[i]);
        }

    }
}
