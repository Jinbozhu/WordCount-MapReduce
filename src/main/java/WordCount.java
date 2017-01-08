import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by feeling on 1/7/17.
 */
public class WordCount {
    public static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
        // key is the offset in the file, indicating which line is being read
        // value is the line being read
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value = "I love studying"
            String[] words = value.toString().split(" ");
            for (String word : words) {
                Text outKey = new Text(word);
                IntWritable outValue = new IntWritable(1);
                context.write(outKey, outValue);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /* For the same key, values are grouped together by Reducer
           For example:
           love 1
           love 1
           -> love <1, 1>
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // key = word
            // values = <1, 1, 1, ...>
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();         // get the value of IntWritable
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);        // mapreduce job
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // tell mapreduce job where the raw data is
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // tell mapreduce job where to store the output data
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
