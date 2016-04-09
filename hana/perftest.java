/**
 * SAP HANA write performance
 * for IoT Sensordata
 */


public class perftest {
    public static void main(String[] argv) {

        final String myname = "KK_STUDENT_030";
        final String mysecret = "M7YaGrq4D3W6aG";
        final String host = "ucchana11.informatik.tu-muenchen.de";
        final String port = "31115";
        final String options = "/?autocommit=false";
        final String connstr = "jdbc:sap://" + host + ":" + port + options;
        System.out.println("Connection String: " + connstr);

        final int iValues = 2; //100000;
        final int iSensors = 2;
        final int VALUE_TYPE_TEMP_CENTIGRADE = 42;
        String tableName = "test";

        final long startTime = System.currentTimeMillis();
        SensorWriter[] s_threads = new SensorWriter[iSensors];

        // Start a thread for every Sensor
        for (int i = 0; i < iSensors; i++) {
            s_threads[i] = new SensorWriter(connstr, myname, mysecret, i, iValues);
            s_threads[i].start();
        }

        // Wait till all Threads are finished
        outerloop:
        while (true) {
            for (int i = 0; i < iSensors; i++) {
                if (s_threads[i].isAlive()) {
                    try {
                        Thread.sleep(100);
                        continue outerloop;
                    } catch (InterruptedException e) {
                        System.err.println("Error while waiting for Threads to finish. Error: " + e);
                    }
                } else {
                    break outerloop;
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
