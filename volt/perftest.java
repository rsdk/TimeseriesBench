import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * SAP HANA write performance
 * for IoT Sensordata
 */


public class perftest {
    public static void main(String[] argv) {

        final String host = "10.0.7.187";


        final int iValues;
        final int iSensors;
        final int buffSize;

        if (argv.length < 3) {
            iValues = 100000;
            iSensors = 8;
            buffSize = 1000;
        } else {
            iValues = Integer.parseInt(argv[0]); // Values pro Sensor
            iSensors = Integer.parseInt(argv[1]);
            buffSize = Integer.parseInt(argv[2]);
        }


        final long startTime = System.currentTimeMillis();
        List<SensorWriter> s_threads = new ArrayList<SensorWriter>(iSensors);

        // Start a thread for every Sensor
        for (int i = 0; i < iSensors; i++) {
            SensorWriter sw = new SensorWriter(host, i, iValues, buffSize);
            sw.start();
            s_threads.add(sw);
        }

        // Wait till all Threads are finished
        while (s_threads.size() > 0) {
            for (int i = s_threads.size() - 1; i >= 0; i--) {
                if (s_threads.get(i).isAlive()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        System.err.println("Error while waiting for Threads to finish. Error: " + e);
                    }
                } else {
                    s_threads.remove(i);
                }
            }
        }


        final int lines = iValues * iSensors;
        final long endTime = System.currentTimeMillis();
        final double delta = (double) endTime - (double) startTime;

        System.out.printf("Runtime: %f.\n", delta);
        System.out.printf("Time per Insert: %8.8f\n", delta / lines);
        System.out.printf("Inserts per Second: %8.8f\n", 1 / (delta / lines) * 1000);
        try (PrintWriter out = new PrintWriter("results.txt")) {
            out.println("voltdb;" + delta + ";" + lines + ";" + iValues + ";" + iSensors + ";" + buffSize);
        } catch (FileNotFoundException e) {
            System.out.println("Error while writing results: " + e);
        }
    }
}
