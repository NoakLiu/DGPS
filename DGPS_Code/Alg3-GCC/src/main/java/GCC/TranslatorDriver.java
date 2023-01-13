
package GCC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class TranslatorDriver extends Configured implements Tool {
    public enum TranslationType {Cluster2Text}
    private final Path input, output;
    private final TranslationType type;

    public TranslatorDriver(TranslationType type, Path input, Path output) {
        this.input = input;
        this.output = output;
        this.type = type;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        new GenericOptionsParser(conf, args);
        Job job = Job.getInstance(conf, "TranslatorDriver " + this.type.toString());
        job.setJarByClass(TranslatorDriver.class);

        job.setNumReduceTasks(0);

        if (this.type == TranslationType.Cluster2Text) {
            job.setMapperClass(Mapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
        }
        FileInputFormat.addInputPath(job, this.input);
        FileOutputFormat.setOutputPath(job, this.output);

        if (!job.waitForCompletion(true))
            return 1;

        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: TranslatorTest <type> <input> <output>");
            System.exit(1);
        }

        TranslationType type;
        type = TranslationType.Cluster2Text;

        Path input = new Path(args[1]);
        Path output = new Path(args[2]);
        System.out.println("Start TranslatorDriver " + type.toString() + ".");
        TranslatorDriver trans = new TranslatorDriver(type, input, output);
        if (trans.run(null) != 0) {
            FileSystem.get(new Configuration()).delete(output, true);
            System.exit(1);
        }
        System.out.println("End TranslatorDriver " + type.toString() + ".");

        System.exit(0);
    }
}