
package GCC;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NodesPairWritable implements WritableComparable<NodesPairWritable> {
    public Integer NodeID = -1;
    public Integer NeighbourID = -1;

    public void readFields(DataInput in) throws IOException {
        this.NodeID = in.readInt();
        this.NeighbourID = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.NodeID);
        out.writeInt(this.NeighbourID);
    }

    public String toString() {
        return this.NodeID + "\t" + this.NeighbourID;
    }

    public int compareTo(NodesPairWritable other) {
        int result = this.NodeID - other.NodeID;
        if (result == 0)
            result = this.NeighbourID - other.NeighbourID;
        return result;
    }

    public int hashCode() {
        int hash1 = (this.NodeID != null) ? this.NodeID.hashCode() : 0;
        int hash2 = (this.NeighbourID != null) ? this.NeighbourID.hashCode() : 0;
        return (hash1 + hash2) * hash2 + hash1;
    }

    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof NodesPairWritable)) return false;

        NodesPairWritable pair = (NodesPairWritable) other;
        boolean cond1 = (this.NodeID != null && pair.NodeID != null && this.NodeID.equals(pair.NodeID));
        boolean cond2 = (this.NeighbourID != null && pair.NeighbourID != null && this.NeighbourID.equals(pair.NeighbourID));
        return cond1 && cond2;
    }
}
