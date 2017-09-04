import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by da on 9/3/17.
 */
public class NormalizeMatrix {
    public static class NormalizeMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: <movieID1>:<movieID2>/t<occurrence>
            // output key:  <movieID1>
            // output value: <movieID2>:<occurrence>
            String[] movieOccurrence = value.toString().trim().split("\t");
            String[] movieIDs = movieOccurrence[0].split(":");

            context.write(new Text(movieIDs[0]), new Text(movieIDs[1] + ":" + movieOccurrence[1]));

        }



    }

    public static class NormalizeMatrixReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input key:  <movieID1>
            // input value: <movieID2>:<occurrence>
            // output key: <movieID2>
            // output value: <movieID1>:<occurrence>/<occurenceSum>

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMatrixMapper.class);
        job.setReducerClass(NormalizeMatrixReducer.class);

        job.setJarByClass(DataPreprocessor.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job,new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
