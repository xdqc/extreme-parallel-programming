package invertedindex;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;


/**
 * Partitioner class that use the maximum word length to partition words based on
 * the word length and the number of reducers so that a word whose length falls in
 * a particular range (e.g. 1-5) goes to a particular partition.
 */
public class InvertedIndexPartitioner extends Partitioner<Text, IntArrayWritable> {

    // maximum length of words
    private final int MAX_LENGTH = 30;

    /**
     * Get partition of a given key
     *
     * @param text the key
     * @param intArrayWritable the output of Mapper
     * @param numPartitions same as number of reducers
     * @return partition index
     */
    @Override
    public int getPartition(Text text, IntArrayWritable intArrayWritable, int numPartitions) {
        return (int)Math.ceil(text.getLength() * numPartitions / (double)MAX_LENGTH) - 1;
    }
}
