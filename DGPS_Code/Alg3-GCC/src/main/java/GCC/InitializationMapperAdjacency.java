
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import GCC.UtilCounters;

public class InitializationMapperAdjacency extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public static final IntWritable MINUS_ONE = new IntWritable(-1);
    private final IntWritable nodeID = new IntWritable();
    private final IntWritable neighbourID = new IntWritable();

    public void map(LongWritable _, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        context.getCounter(UtilCounters.NUM_INITIAL_NODES).increment(1);

        String[] userID_neighbourhood = line.split("\t");

        nodeID.set(Integer.parseInt(userID_neighbourhood[0]));

        if (userID_neighbourhood.length == 1) {
            context.write(nodeID, MINUS_ONE);
            return;
        }

        String[] neighbours = userID_neighbourhood[1].split(",");

        for (String neighbour : neighbours) {
            neighbourID.set(Integer.parseInt(neighbour));
            if (nodeID.get() > neighbourID.get())
                context.write(nodeID, neighbourID);
        }
    }
}
