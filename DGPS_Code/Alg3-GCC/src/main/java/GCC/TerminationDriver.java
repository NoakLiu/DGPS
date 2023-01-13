
package GCC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class TerminationDriver extends Configured implements Tool {
    private final Path input, output;
    private final boolean verbose;
    private long numNodes, numClusters;

    public TerminationDriver(Path input, Path output, boolean verbose) {
        this.input = input;
        this.output = output;
        this.verbose = verbose;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        new GenericOptionsParser(conf, args);
        Job job = Job.getInstance(conf, "TerminationDriver");
        job.setJarByClass(TerminationDriver.class);

        job.setMapOutputKeyClass(NodesPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(ClusterWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(TerminationMapper.class);
        job.setPartitionerClass(NodePartitioner.class);
        job.setGroupingComparatorClass(NodeGroupingComparator.class);
        job.setReducerClass(TerminationReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, this.input);
        FileOutputFormat.setOutputPath(job, this.output);

        if (!job.waitForCompletion(this.verbose))
            return 1;

        this.numNodes = job.getCounters().findCounter(UtilCounters.NUM_NODES).getValue();
        this.numClusters = job.getCounters().findCounter(UtilCounters.NUM_CLUSTERS).getValue();
        return 0;
    }

    public long getNumNodes() {
        return this.numNodes;
    }

    public long getNumClusters() {
        return this.numClusters;
    }


}
