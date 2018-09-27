package invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The combiner to reduce the amount of data being shuffled
 */
public class InvertedIndexCombiner extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {
    private int documentId = 0;
    private int lineId = 0;
    private int sentencePosition = 0;

    protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> wordPositions = new ArrayList<>();

        // combine mapper output values with the same key
        for (IntArrayWritable x : values) {
            documentId = ((IntWritable) x.get()[0]).get();
            lineId = ((IntWritable) x.get()[1]).get();
            sentencePosition = ((IntWritable) x.get()[2]).get();

            wordPositions.add(documentId);
            wordPositions.add(lineId);
            wordPositions.add(sentencePosition);
        }
        context.write(key, new IntArrayWritable(wordPositions.stream().mapToInt(i -> i).toArray()));
    }
}
