
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class StarCombiner extends Reducer<NodesPairWritable, IntWritable, NodesPairWritable, IntWritable> {
    public void reduce(NodesPairWritable pair, Iterable<IntWritable> neighbourhood, Context context) throws IOException, InterruptedException {
        int lastNodeSeen = -2;
        for (IntWritable neighbour : neighbourhood) {
            if (neighbour.get() == lastNodeSeen)
                continue;

            pair.NeighbourID = neighbour.get();
            context.write(pair, neighbour);

            lastNodeSeen = neighbour.get();
        }
    }
}
