
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CheckMapper extends Mapper<ClusterWritable, NullWritable, IntWritable, NullWritable> {
    private static final NullWritable NULL = NullWritable.get();
    private final IntWritable nodeID = new IntWritable();

    public void map(ClusterWritable cluster, NullWritable value, Context context) throws IOException, InterruptedException {
        for (Integer node : cluster) {
            nodeID.set(node);
            context.write(nodeID, NULL);
        }
    }
}