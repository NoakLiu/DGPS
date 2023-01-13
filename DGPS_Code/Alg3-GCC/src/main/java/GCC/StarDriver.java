
package GCC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class StarDriver extends Configured implements Tool {
    public enum StarDriverType {LARGE, SMALL}

    ;

    private final String title;
    private final StarDriverType type;
    private final Path input, output;
    private final boolean verbose;
    private long numChanges;

    public StarDriver(StarDriverType type, Path input, Path output, int iteration, boolean verbose) {
        this.type = type;
        this.title = type.equals(StarDriverType.SMALL) ? "Small-Star" + iteration : "Large-Star" + iteration;
        this.input = input;
        this.output = output;
        this.verbose = verbose;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        new GenericOptionsParser(conf, args);
        conf.set("type", this.type.toString());
        Job job = Job.getInstance(conf, this.title);
        job.setJarByClass(StarDriver.class);

        job.setMapOutputKeyClass(NodesPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(StarMapper.class);
        job.setCombinerClass(StarCombiner.class);
        job.setPartitionerClass(NodePartitioner.class);
        job.setGroupingComparatorClass(NodeGroupingComparator.class);
        job.setReducerClass(StarReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, this.input);
        FileOutputFormat.setOutputPath(job, this.output);

        if (!job.waitForCompletion(verbose))
            return 1;

        this.numChanges = job.getCounters().findCounter(UtilCounters.NUM_CHANGES).getValue();
        return 0;
    }

    public long getNumChanges() {
        return this.numChanges;
    }

}
