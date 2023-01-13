
package GCC;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import GCC.UtilCounters;

public class CheckReducer extends Reducer<IntWritable, NullWritable, NullWritable, NullWritable> {
    public void reduce(IntWritable nodeID, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for (NullWritable ignored : values) {
            count++;
            if (count > 1) {
                context.getCounter(UtilCounters.NUM_ERRORS).increment(1);
                break;
            }
        }
    }
}