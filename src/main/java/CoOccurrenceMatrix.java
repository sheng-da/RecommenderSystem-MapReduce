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
public class CoOccurrenceMatrix {
    public static class CoOccurrenceMatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value:  <userid>/t<movieID>:<rating>,<movieID>:<rating>...
            // output key:  <movieID1>:<movieID2>
            // output value: 1

            String line = value.toString().trim();
            String[] userRating = line.split("\t");
            String[] movieRatings = userRating[1].split(",");

            for (int i = 0; i < movieRatings.length; i++) {
                String movieId1 = movieRatings[i].trim().split(":")[0];

                for (int j = 0; j < movieRatings.length; j++) {
                    String movieId2 = movieRatings[j].trim().split(":")[0];
                    context.write(new Text(movieId1+":"+movieId2), new IntWritable(1));
                }
            }

        }



    }

    public static class CoOccurrenceMatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //sum up
            int sum = 0;
            while(values.iterator().hasNext()) {
                sum += values.iterator().next().get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(CoOccurrenceMatrixGeneratorMapper.class);
        job.setReducerClass(CoOccurrenceMatrixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrix.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job,new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
