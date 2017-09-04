import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by da on 9/4/17.
 */
public class FinalSumUp {
    public static class FinalSumUpMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: <userId>:<movieId>/t<normalized-coocurrence * rating>

            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]), new DoubleWritable(Double.parseDouble(line[1])));

        }

    }

    public static class FinalSumUpReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // output value:  <userId>:<movieId>/t<recommendation score>

            double sum = 0;
            for (DoubleWritable value: values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(FinalSumUpMapper.class);
        job.setReducerClass(FinalSumUpReducer.class);

        job.setJarByClass(FinalSumUp.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job,new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
