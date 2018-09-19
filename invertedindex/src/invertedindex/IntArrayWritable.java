package invertedindex;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() { super(IntWritable.class); }

    public IntArrayWritable(int[] ints) {
        super(IntWritable.class);

        IntWritable[] intWritables = new IntWritable[ints.length];
        for (int i = 0; i < ints.length; i++) {
            intWritables[i] = new IntWritable(ints[i]);
        }
        set(intWritables);
    }

}