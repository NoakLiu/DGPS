
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TerminationMapper extends Mapper<IntWritable, IntWritable, NodesPairWritable, IntWritable> {
    private final NodesPairWritable pair = new NodesPairWritable();

    public void map(IntWritable nodeID, IntWritable neighbourID, Context context) throws IOException, InterruptedException {
        if (nodeID.get() < neighbourID.get() || neighbourID.get() == -1) {
            pair.NodeID = nodeID.get();
            pair.NeighbourID = neighbourID.get();

            context.write(pair, neighbourID);
        } else {
            pair.NodeID = neighbourID.get();
            pair.NeighbourID = nodeID.get();

            context.write(pair, nodeID);
        }
    }
}
