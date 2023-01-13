
package GCC;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NodeGroupingComparator extends WritableComparator {
    protected NodeGroupingComparator() {
        super(NodesPairWritable.class, true);
    }

    public int compare(WritableComparable key1, WritableComparable key2) {
        NodesPairWritable pair1 = (NodesPairWritable) key1;
        NodesPairWritable pair2 = (NodesPairWritable) key2;

        return pair1.NodeID - pair2.NodeID;
    }
}