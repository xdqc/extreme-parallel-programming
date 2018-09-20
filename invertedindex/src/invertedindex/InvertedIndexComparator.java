package invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvertedIndexComparator extends WritableComparator {
    public InvertedIndexComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String w1 = a.toString();
        String w2 = b.toString();
        return Integer.compare(w2.length(), w1.length());
    }
}
