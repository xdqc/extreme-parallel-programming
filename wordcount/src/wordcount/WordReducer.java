package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        // Loop over the input values, summing them up.
        int sum = 0;
        for (IntWritable x : values) {
            sum += x.get();
        }

        // Only care about words occur more than 500 times
        if (sum >= 500) {
            context.write(key, new IntWritable(sum));
        }
    }
}
