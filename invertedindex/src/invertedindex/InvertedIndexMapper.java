package invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * The mapper class for inverted indexer
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

    private Text word = new Text();
    private int documentId = 0;
    private int lineId = 0;
    private int sentencePosition = 0;

    // Use HashSet for fast search stopwords
    private static HashSet<String> stopwords = new HashSet<>();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        stopwords.addAll(Arrays.asList(conf.getStrings("stopwords")));

        String line = value.toString();
        lineId++;

        if (line.indexOf("<Document") == 0) {
            documentId = Integer.parseInt(line.split("\"")[1]);
            lineId = 0;
        }

        sentencePosition = 0;

        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            // Skip stopwords
            if (stopwords.contains(token)){
                continue;
            }

            word.set(token);
            context.write(word, new IntArrayWritable(new int[]{documentId, lineId, sentencePosition}));
            sentencePosition++;
        }
    }
}