
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import GCC.UtilCounters;

public class TerminationReducer extends Reducer<NodesPairWritable, IntWritable, ClusterWritable, NullWritable> {
    private static final NullWritable NULL = NullWritable.get();
    private final ClusterWritable cluster = new ClusterWritable();

    public void reduce(NodesPairWritable pair, Iterable<IntWritable> neighbourhood, Context context) throws IOException, InterruptedException {
        cluster.clear();

        cluster.add(pair.NodeID);

        if (pair.NeighbourID != -1) {
            int lastNodeSeen = -2;

            for (IntWritable neighbour : neighbourhood) {
                if (neighbour.get() == lastNodeSeen)
                    continue;

                cluster.add(neighbour.get());

                lastNodeSeen = neighbour.get();
            }
        }

        context.getCounter(UtilCounters.NUM_NODES).increment(cluster.size());
        context.getCounter(UtilCounters.NUM_CLUSTERS).increment(1);
        context.write(cluster, NULL);
    }
}
