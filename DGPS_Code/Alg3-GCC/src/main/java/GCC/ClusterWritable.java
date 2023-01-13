
package GCC;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ClusterWritable extends ArrayList<Integer> implements Writable {
    private static final long serialVersionUID = 1L;

    public ArrayList<Integer> Cluster = new ArrayList<Integer>();

    public ClusterWritable() {
        super();
    }

    public ClusterWritable(ArrayList<Integer> array) {
        super(array);
    }

    public void readFields(DataInput in) throws IOException {
        this.clear();

        int numFields = in.readInt();
        if (numFields == 0) return;

        for (int i = 0; i < numFields; i++)
            this.add(in.readInt());
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.size());
        if (size() == 0) return;

        for (Integer integer : this) out.writeInt(integer);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (this.size() > 0) {
            sb.append(this.get(0));
            for (int i = 1; i < this.size(); i++)
                sb.append(" ").append(this.get(i));
        }
        return sb.toString();
    }
}