package invertedindex;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * The writable class to hold integer array.
 *
 * In this assignment, this class serve as the output of Mapper, input of Reducer, and the input&output of Combiner,
 * the elements if int array represent the documentId, lindId and sentencePosition.
 */
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