
package GCC;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NodePartitioner extends Partitioner<NodesPairWritable, IntWritable>
{
		public int getPartition(NodesPairWritable pair, IntWritable value, int numPartitions )
	{
		return pair.NodeID % numPartitions;
	}
}