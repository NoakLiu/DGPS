
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import GCC.UtilCounters;

public class InitializationMapperClique extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private static final IntWritable MINUS_ONE = new IntWritable(-1);
    private final IntWritable nodeID = new IntWritable();
    private final IntWritable neighbourID = new IntWritable();
    private MultipleOutputs<IntWritable, IntWritable> mos = null;

    protected void setup(Context context) throws IOException, InterruptedException {
        this.mos = new MultipleOutputs<IntWritable, IntWritable>(context);
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        context.getCounter(UtilCounters.NUM_CLIQUES).increment(1);

        String[] cliquesLists = line.split(" ");

        if (cliquesLists.length == 1) {
            nodeID.set(Integer.parseInt(cliquesLists[0]));
            context.write(nodeID, MINUS_ONE);
            mos.write(nodeID, MINUS_ONE, GCC.InitializationDriver.MOS_BASEOUTPUTPATH);
            return;
        }

        for (int i = 0; i < cliquesLists.length - 1; i++) {
            int nodeX = Integer.parseInt(cliquesLists[i]);

            for (int j = i + 1; j < cliquesLists.length; j++) {
                int nodeY = Integer.parseInt(cliquesLists[j]);

                nodeID.set(Math.max(nodeX, nodeY));
                neighbourID.set(Math.min(nodeX, nodeY));
                mos.write(nodeID, neighbourID, GCC.InitializationDriver.MOS_BASEOUTPUTPATH);
            }

            nodeID.set(nodeX);
            context.write(nodeID, MINUS_ONE);
        }
        nodeID.set(Integer.parseInt(cliquesLists[cliquesLists.length - 1]));
        context.write(nodeID, MINUS_ONE);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.mos.close();
    }
}
