package invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;

public class InvertedIndexReducer extends Reducer<Text, IntArrayWritable, Text, IntWritable> {
    private int documentId = 0;
    private int lineId = 0;
    private int sentencePosition = 0;

    public void reduce(Text key, Iterable<IntArrayWritable> values,
                       Context context) throws IOException, InterruptedException {

        // Use TreeMap & TreeSet to keep the order of documentId and lindId
        TreeMap<Integer, TreeSet<SentencePosition>> documentCount = new TreeMap<>();

        int sum = 0;
        for (IntArrayWritable x : values) {
            documentId = ((IntWritable)x.get()[0]).get();
            lineId = ((IntWritable)x.get()[1]).get();
            sentencePosition = ((IntWritable)x.get()[2]).get();
            sum++;

            if (documentCount.containsKey(documentId)){
                TreeSet<SentencePosition> line = documentCount.get(documentId);
                line.add(new SentencePosition(lineId, sentencePosition));
            } else {
                TreeSet<SentencePosition> line = new TreeSet<>();
                line.add(new SentencePosition(lineId, sentencePosition));
                documentCount.put(documentId, line);
            }
        }

        context.write(key, new IntWritable(sum));
        for (int docId: documentCount.keySet()) {
            Text text = new Text();
            text.set("\t"+String.valueOf(docId));
            TreeSet<SentencePosition> lines = documentCount.get(docId);
            context.write(text, new IntWritable(lines.size()));

            for (SentencePosition pos : lines) {
                Text tx = new Text();
                tx.set("\t\t"+String.valueOf(pos.lineId));
                context.write(tx, new IntWritable(pos.wordPos));
            }
        }

        // User defined counter
        if (isPalindrome(key.toString())) {
            context.getCounter(InterestingCounter.Palindromes).increment(1);
        }
    }

    /**
     * Check if a string is a palindrome
     */
    private boolean isPalindrome(String word){
        int length = word.length();
        if (length <= 2) return false;

        for (int i = 0; i < length / 2; i++) {
            if (word.charAt(i) == word.charAt(length-i-1)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A Tuple2<Integer, Integer> as comparable element of TreeSet
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
