package invertedindex;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

    private Text word = new Text();
    private int documentId = 0;
    private int lineId = 0;
    private int sentencePosition = 0;

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        lineId++;

        if (line.indexOf("<Document") == 0) {
            documentId = Integer.parseInt(line.split("\"")[1]);
            lineId = 0;
        }

        sentencePosition = 0;

        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, new IntArrayWritable(new int[]{documentId, lineId, sentencePosition}));
            sentencePosition++;
        }
    }
}