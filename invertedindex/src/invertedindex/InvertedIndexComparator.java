package invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * The comparator class that sorts words by length in a descending order.
 */
public class InvertedIndexComparator extends WritableComparator {
    public InvertedIndexComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return Integer.compare(b.toString().length(), a.toString().length());
    }
}
