package invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The reducer class for inverted indexer,
 */
public class InvertedIndexReducer extends Reducer<Text, IntArrayWritable, Text, IntWritable> {
    private int documentId = 0;
    private int lineId = 0;
    private int sentencePosition = 0;

    /**
     * Reduce the output, with updating of customized counter
     */
    public void reduce(Text key, Iterable<IntArrayWritable> values,
                       Context context) throws IOException, InterruptedException {

        // Use TreeMap & TreeSet to keep the order of documentId and lindId
        TreeMap<Integer, TreeSet<SentencePosition>> invertedIndex = new TreeMap<>();

        int sum = 0;
        for (IntArrayWritable x : values) {
            // every 3 consecutive elements of the IntArrayWritable represent the documentId, lindId and sentencePosition respectively
            for (int i = 0; i < x.get().length; i += 3) {
                documentId = ((IntWritable) x.get()[i]).get();
                lineId = ((IntWritable) x.get()[i + 1]).get();
                sentencePosition = ((IntWritable) x.get()[i + 2]).get();
                sum++;

                if (invertedIndex.containsKey(documentId)) {
                    TreeSet<SentencePosition> line = invertedIndex.get(documentId);
                    line.add(new SentencePosition(lineId, sentencePosition));
                } else {
                    TreeSet<SentencePosition> line = new TreeSet<>();
                    line.add(new SentencePosition(lineId, sentencePosition));
                    invertedIndex.put(documentId, line);
                }
            }
        }

        // Write tree-like output (the Inverted Index)
        context.write(key, new IntWritable(sum));
        for (int docId : invertedIndex.keySet()) {
            Text text = new Text();
            text.set("\t" + String.valueOf(docId));
            TreeSet<SentencePosition> lines = invertedIndex.get(docId);
            context.write(text, new IntWritable(lines.size()));

            for (SentencePosition pos : lines) {
                Text tx = new Text();
                tx.set("\t\t" + String.valueOf(pos.lineId));
                context.write(tx, new IntWritable(pos.wordPos));
            }
        }

        // Update user defined counter
        if (key.toString().matches("^[0-9]+$")) {
            context.getCounter("Pure number", "First digit " + key.toString().charAt(0)).increment(1);
        } else if (isPalindrome(key.toString())) {
            context.getCounter(InterestingCounter.Palindromes).increment(1);
        }
    }

    /**
     * Check whether a string is a palindrome
     */
    private boolean isPalindrome(String word) {
        int length = word.length();
        if (length <= 2) return false;

        // ignore case
        word = word.toLowerCase();

        for (int i = 0; i < length / 2; i++) {
            if (word.charAt(i) != word.charAt(length - i - 1)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A Tuple2 implements comparable as the element of TreeSet
     */
    private class SentencePosition implements Comparable<SentencePosition> {
        final int lineId;
        final int wordPos;

        public SentencePosition(int x, int y) {
            this.lineId = x;
            this.wordPos = y;
        }

        @Override
        public int compareTo(SentencePosition o) {
            return Integer.compare(this.lineId, o.lineId);
        }
    }

}
