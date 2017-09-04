import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by da on 9/4/17.
 */
public class MatrixMultiplier {
    public static class CoocurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: <movieID2>\t<movieID1>=<normalized-occurrence>
            // output key:  <movieID2>
            // output value: <movieID1>=<normalized-coocurrence>

            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: <userid>,<movieID>,<rating>
            // output key:  <movieID>
            // output value: <userid>:<rating>

            String[] userRatings = value.toString().split(",");
            context.write(new Text(userRatings[1]), new Text(userRatings[0] + ":" + userRatings[2]));
        }
    }

    public static class MatrixMultiplierReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        // output key: <userId>:<movieId>
        // output value: <normalized-coocurrence * rating>
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, Double> cooccurrenceMap = new HashMap<String,Double>(); //movieID, coocurrence
            Map<String, Double> ratingsMap= new HashMap<String, Double>(); // movieID,rating

            // save the data to the map
            for (Text value: values) {
                if (value.toString().contains("=")) { // coocurrence
                    String[] coocurrence = value.toString().split("=");
                    cooccurrenceMap.put(coocurrence[0],Double.parseDouble(coocurrence[1]));
                } else { // rating
                    String[] rating = value.toString().split(":");
                    ratingsMap.put(rating[0],Double.parseDouble(rating[1]));
                }
            }


            for (Map.Entry<String,Double> entry: cooccurrenceMap.entrySet()) {
                String movieId = entry.getKey();
                double coocurrence = entry.getValue();

                for (Map.Entry<String,Double> ratingEntry: ratingsMap.entrySet()) {
                    String userId = ratingEntry.getKey();
                    double rating = ratingEntry.getValue();
                    context.write(new Text(userId + ":" + movieId), new DoubleWritable(rating*coocurrence));
                }
            }
        }
    }

    /**
     * @param args input input output
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        ChainMapper.addMapper(job,CoocurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job,RatingsMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(RatingsMapper.class);
        job.setMapperClass(CoocurrenceMapper.class);

        job.setReducerClass(MatrixMultiplierReducer.class);

        job.setJarByClass(MatrixMultiplier.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoocurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingsMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
