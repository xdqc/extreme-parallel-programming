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
        String w1 = a.toString();
        String w2 = b.toString();
        if (w1.length() == w2.length()) {
            // prevent the comparator combining two keys with same length
            return w1.compareTo(w2);
        } else {
            return Integer.compare(w2.length(), w1.length());
        }
    }
}
