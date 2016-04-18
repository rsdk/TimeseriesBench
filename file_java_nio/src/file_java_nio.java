import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Random;

/**
 * Write the same data to file to estimate the raw io-performance
 * Created by rene on 18.04.16.
 */

public class file_java_nio {
    public static void main(String[] args) throws IOException {
        final int sid_n = 10;
        final int n_values = 1000 * 1000 * 10;
        final int bufsize = 10000;

        final Random rand = new Random();

        LocalDateTime ldt;
        Timestamp ts;

        //prepare arrays with values
        long t_arr[] = new long[n_values];
        double value_arr[] = new double[n_values];
        for (int i = 0; i < n_values; i++) {
            ldt = LocalDateTime.now();
            ts = Timestamp.valueOf(ldt);
            t_arr[i] = ts.getTime();
            value_arr[i] = rand.nextDouble();
        }

        FileOutputStream fout = new FileOutputStream("mytestfile.bin");
        FileChannel fc = fout.getChannel();

        ByteBuffer buffer = ByteBuffer.allocate((4 + 8 + 8) * bufsize);

        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < n_values; i++) {

            buffer.putInt(i % sid_n);         //4
            buffer.putLong(t_arr[i]);          //8
            buffer.putDouble(value_arr[i]);    //8
            if (i % bufsize == 0) {
                buffer.flip();
                fc.write(buffer);
                buffer.clear();
            }
        }
        buffer.flip();
        fc.write(buffer);
        fc.force(true); // write everything to disk
        fc.close();
        final long endTime = System.currentTimeMillis();

        final double delta = (double) endTime - (double) startTime;
        System.out.printf("Runtime in ms: %8.3f.\n", delta);
        System.out.printf("Time per row in ms: %8.6f\n", delta / n_values);
        System.out.printf("Row-Inserts per s: %8.2f\n", 1 / (delta / n_values) * 1000);
        System.out.printf("MB/s: %8.2f\n", ((4 + 8 + 8) * n_values / 1000) / delta);
    }
}
