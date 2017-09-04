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
public class DataPreprocessor {
    public static class DataPreprocessorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: <userid>,<movieID>,<rating>
            // output key:  <userid>
            // output value: <movieID>:<rating>
            String[] userRating = value.toString().split(",");
            int userId = Integer.parseInt(userRating[0]);
            String movieId = userRating[1];
            String rating = userRating[2];

            context.write(new IntWritable(userId),new Text(movieId + ":" + rating));
        }



    }

    public static class DataPreprocessorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input key: <userid>
            // input value: <movieID>:<rating>
            // output key:  <userid>
            // output value: <movieID>:<rating>,<movieID>:<rating>...
            StringBuilder sb = new StringBuilder();
            while (values.iterator().hasNext()) {
                sb.append(",");
                sb.append(values.iterator().next());
            }

            String res = sb.toString();
            if (res == null || res.equals("")) return;

            context.write(key, new Text(sb.toString().replaceFirst(",","")));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(DataPreprocessorMapper.class);
        job.setReducerClass(DataPreprocessorReducer.class);

        job.setJarByClass(DataPreprocessor.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job,new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
