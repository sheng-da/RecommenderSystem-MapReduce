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
import java.util.HashMap;
import java.util.Map;

/**
 * Created by da on 9/3/17.
 */
public class NormalizeMatrix {
    public static class NormalizeMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input key:  <movieID1>
            // input value: <movieID2>:<occurrence>
            // output key: <movieID2>
            // output value: <movieID1>= <(occurrence>/<occurenceSum)>
            int occurenceSum = 0;
            Map<String, Integer> res = new HashMap<String, Integer>();

            // save the data to the map and calculate the sum
            while(values.iterator().hasNext()) {
                String[] movieOccurrence = values.iterator().next().toString().split(":");
                int occurrence = Integer.parseInt(movieOccurrence[1]);
                occurenceSum += occurrence;
                res.put(movieOccurrence[0],occurrence);
            }

            // normalize
            for (Map.Entry<String, Integer> entry: res.entrySet()) {
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double)entry.getValue()/occurenceSum;
                context.write(new Text(outputKey), new Text(outputValue));
            }



        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMatrixMapper.class);
        job.setReducerClass(NormalizeMatrixReducer.class);

        job.setJarByClass(NormalizeMatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job,new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
