
package GCC;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class InitializationDriver extends Configured implements Tool {
    public enum InputType {ADJACENCY_LIST, CLIQUES_LIST}

    ;
    public static final String MOS_OUTPUT_NAME = "result";
    public static final String MOS_BASEOUTPUTPATH = MOS_OUTPUT_NAME + "/part";

    private final Path input, output;
    private final boolean verbose;
    private InputType type;
    private long numCliques, numInitialNodes;

    public InitializationDriver(Path input, Path output, boolean verbose) throws IOException {
        this.input = input;
        this.output = output;
        this.verbose = verbose;

        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(this.input)));

        boolean done = false;
        while (!done) {
            String line = br.readLine();
            String[] userID_neighborhood = line.split("\t");
            if (userID_neighborhood.length == 1) {
                String[] cliquesLists = line.split(" ");

                if (cliquesLists.length > 1) {
                    this.type = InputType.CLIQUES_LIST;
                    done = true;
                }
            } else {
                this.type = InputType.ADJACENCY_LIST;
                done = true;
            }
        }

        br.close();
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        new GenericOptionsParser(conf, args);
        Job job = Job.getInstance(conf, "InitializationDriver");
        job.setJarByClass(InitializationDriver.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, this.input);
        FileOutputFormat.setOutputPath(job, this.output);

        if (this.type == InputType.ADJACENCY_LIST) {
            job.setMapperClass(InitializationMapperAdjacency.class);
            job.setNumReduceTasks(0);
        } else {
            MultipleOutputs.addNamedOutput(job, MOS_OUTPUT_NAME, SequenceFileOutputFormat.class, IntWritable.class, IntWritable.class);
            MultipleOutputs.setCountersEnabled(job, true);
            job.setMapperClass(InitializationMapperClique.class);
            job.setCombinerClass(InitializationCombinerNumNodes.class);
            job.setReducerClass(InitializationReducerNumNodes.class);
        }

        if (!job.waitForCompletion(verbose))
            return 1;

        this.numCliques = job.getCounters().findCounter(UtilCounters.NUM_CLIQUES).getValue();
        this.numInitialNodes = job.getCounters().findCounter(UtilCounters.NUM_INITIAL_NODES).getValue();

        if (this.type == InputType.CLIQUES_LIST) {
            FileSystem fs = FileSystem.get(conf);

            FileStatus[] filesStatus = fs.listStatus(this.output);
            for (FileStatus fileStatus : filesStatus)
                if (fileStatus.getPath().getName().contains("part"))
                    fs.delete(fileStatus.getPath(), false);

            filesStatus = fs.listStatus(this.output.suffix("/" + MOS_OUTPUT_NAME));
            for (FileStatus fileStatus : filesStatus)
                fs.rename(fileStatus.getPath(), this.output.suffix("/" + fileStatus.getPath().getName()));

            fs.delete(this.output.suffix("/" + MOS_OUTPUT_NAME), true);
        }

        return 0;
    }

    public InputType getInputType() {
        return this.type;
    }

    public long getNumCliques() {
        return this.numCliques;
    }

    public long getNumInitialNodes() {
        return this.numInitialNodes;
    }

}
