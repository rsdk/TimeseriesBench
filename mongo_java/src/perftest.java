/**
 * mysql write performance
 * for IoT Sensordata
 */


public class perftest {
    public static void main(String[] argv) {

        final String myname = "root";
        final String mysecret = "dc-mysql";
        final String host = "127.0.0.1";
        final String port = "27017";
        final String dbname = "/test";
        final String options = "?autoReconnect=true&useSSL=false";
        final String connstr = "jdbc:mongo://" + host; // + ":" + port + dbname + options;
        System.out.println("Connection String: " + connstr);

        final int iValues;
        final int iSensors;
        final int buffSize;
        if (argv.length < 3) {
            iValues = 100000;
            iSensors = 10;
            buffSize = 1000;
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
        System.out.printf("Time per Insert: %8.8f\n", delta / lines);
        System.out.printf("Inserts per Second: %8.8f\n", 1 / (delta / lines) * 1000);
    }
}
