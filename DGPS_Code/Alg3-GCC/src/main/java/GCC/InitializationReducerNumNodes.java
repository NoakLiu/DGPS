
package GCC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import GCC.UtilCounters;

public class InitializationReducerNumNodes extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
{
		public void reduce(IntWritable nodeID, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
	{
		context.getCounter( UtilCounters.NUM_INITIAL_NODES ).increment( 1 );
	}
}