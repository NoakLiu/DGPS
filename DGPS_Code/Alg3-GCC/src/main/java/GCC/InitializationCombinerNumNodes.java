
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class InitializationCombinerNumNodes extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public static final IntWritable MINUS_ONE = new IntWritable(-1);

    public void reduce(IntWritable nodeID, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(nodeID, MINUS_ONE);
    }
}