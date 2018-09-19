package invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeMap;

public class InvertedIndexReducer extends Reducer<Text, IntArrayWritable, Text, IntWritable> {
    private int documentId = 0;
    private int lineId = 0;
    private int sentencePosition = 0;

    public void reduce(Text key, Iterable<IntArrayWritable> values,
                       Context context) throws IOException, InterruptedException {

        TreeMap<Integer, ArrayList<int[]>> documentCount = new TreeMap<>();

        int sum = 0;
        for (IntArrayWritable x : values) {
            documentId = ((IntWritable)x.get()[0]).get();
            lineId = ((IntWritable)x.get()[1]).get();
            sentencePosition = ((IntWritable)x.get()[2]).get();
            sum++;

            if (documentCount.containsKey(documentId)){
                ArrayList<int[]> line = documentCount.get(documentId);
                line.add(new int[]{lineId, sentencePosition});
            } else {
                ArrayList<int[]> line = new ArrayList<>();
                line.add(new int[]{lineId, sentencePosition});
                documentCount.put(documentId, line);
            }
        }

        context.write(key, new IntWritable(sum));
        for (int docId: documentCount.keySet()) {
            Text text = new Text();
            text.set("\t"+String.valueOf(docId));
            ArrayList<int[]> lines = documentCount.get(docId);
            context.write(text, new IntWritable(lines.size()));

            lines.sort(Comparator.comparingInt(o -> o[0]));

            for (int[] pos : lines) {
                Text tx = new Text();
                tx.set("\t\t"+String.valueOf(pos[0]));
                context.write(tx, new IntWritable(pos[1]));
            }
        }
    }
}
