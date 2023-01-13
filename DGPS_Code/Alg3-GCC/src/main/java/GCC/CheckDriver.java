
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

public class CheckDriver extends Configured implements Tool {
    private final Path input;
    private final boolean verbose;
    private boolean testOk;

    public CheckDriver(Path input, boolean verbose) {
        this.input = input;
        this.verbose = verbose;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        new GenericOptionsParser(conf, args);
        Job job = Job.getInstance(conf, "CheckDriver");
        job.setJarByClass(CheckDriver.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(CheckMapper.class);
        job.setReducerClass(CheckReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, this.input);
        FileOutputFormat.setOutputPath(job, this.input.suffix("_check"));

        if (!job.waitForCompletion(verbose))
            return 1;

        this.testOk = (job.getCounters().findCounter(UtilCounters.NUM_ERRORS).getValue() == 0);

        FileSystem.get(conf).delete(input.suffix("_check"), true);

        return 0;
    }

    public boolean isTestOk() {
        return this.testOk;
    }

}