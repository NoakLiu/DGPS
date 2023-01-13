
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class StarMapper extends Mapper<IntWritable, IntWritable, NodesPairWritable, IntWritable> {
    private boolean smallStar;
    private final NodesPairWritable pair = new NodesPairWritable();

    public void setup(Context context) {
        smallStar = context.getConfiguration().get("type").equals("SMALL");
    }

    public void map(IntWritable nodeID, IntWritable neighbourID, Context context) throws IOException, InterruptedException {
        if (neighbourID.get() == -1) {
            pair.NodeID = nodeID.get();
            pair.NeighbourID = neighbourID.get();

            context.write(pair, neighbourID);
            return;
        }

        if (smallStar) {
            if (neighbourID.get() < nodeID.get()) {
                pair.NodeID = nodeID.get();
                pair.NeighbourID = neighbourID.get();

                context.write(pair, neighbourID);
            } else {
                pair.NodeID = neighbourID.get();
                pair.NeighbourID = nodeID.get();

                context.write(pair, nodeID);
            }
        } else {
            pair.NodeID = nodeID.get();
            pair.NeighbourID = neighbourID.get();

            context.write(pair, neighbourID);

            pair.NodeID = neighbourID.get();
            pair.NeighbourID = nodeID.get();

            context.write(pair, nodeID);
        }
    }
}
