package invertedindex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.nio.file.Files;
import java.nio.file.Paths;

public class InvertedIndex extends Configured implements Tool {
    private static int numReduces = 8;
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("stopwords", Files.readAllLines(Paths.get("stopwords_google.txt")).toArray(new String[0]));

        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(InvertedIndex.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setSortComparatorClass(InvertedIndexComparator.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setNumReduceTasks(numReduces);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        final int result;
        if (args.length < 2) {
            System.out.println("Arguments: inputDirectory outputDirectory numReducer");
            result = -1;
        } else {
            // We convert args[2] to number of reducers


            result = ToolRunner.run(new InvertedIndex(), args);
        }
        System.exit(result);
    }
}