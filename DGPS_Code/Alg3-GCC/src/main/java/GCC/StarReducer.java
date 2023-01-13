
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import GCC.UtilCounters;

public class StarReducer extends Reducer<NodesPairWritable, IntWritable, IntWritable, IntWritable> {
    private static final IntWritable MINUS_ONE = new IntWritable(-1);
    private final IntWritable nodeID = new IntWritable();
    private final IntWritable minNodeID = new IntWritable();
    private boolean smallStar;

    public void setup(Context context) {
        smallStar = context.getConfiguration().get("type").equals("SMALL");
    }

    public void reduce(NodesPairWritable pair, Iterable<IntWritable> neighbourhood, Context context) throws IOException, InterruptedException {
        long numProducedPairs = 0;

        if (pair.NeighbourID == -1) {
            minNodeID.set(pair.NodeID);
            context.write(minNodeID, MINUS_ONE);
            return;
        }

        minNodeID.set(Math.min(pair.NodeID, pair.NeighbourID));

        if (smallStar && (pair.NodeID != minNodeID.get())) {
            nodeID.set(pair.NodeID);
            context.write(nodeID, minNodeID);
        }

        int lastNodeSeen = -2;
        for (IntWritable neighbour : neighbourhood) {
            if (neighbour.get() == lastNodeSeen)
                continue;

            boolean cond = (smallStar ? (neighbour.get() != minNodeID.get()) : (neighbour.get() > pair.NodeID));

            if (cond) {
                context.write(neighbour, minNodeID);
                numProducedPairs++;
            }

            lastNodeSeen = neighbour.get();
        }

        if (pair.NodeID != minNodeID.get())
            context.getCounter(UtilCounters.NUM_CHANGES).increment(numProducedPairs);
    }
}
