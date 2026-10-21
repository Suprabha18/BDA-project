
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebLogAnalysis {

    public static class LogMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text outputKey = new Text();

        
private static final Pattern LOG_PATTERN = Pattern.compile(
    "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[[^\\]]+\\]\\s+\"([A-Z]+)\\s+(\\S+)\\s+HTTP/1\\.1\"\\s+(\\d{3})\\s+\\S+"
);


        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Matcher matcher = LOG_PATTERN.matcher(value.toString());

            if (matcher.find()) {
                String ip = matcher.group(1);
                String url = matcher.group(3);
                String status = matcher.group(4);

                outputKey.set("TOTAL_REQUESTS");
                context.write(outputKey, ONE);

                outputKey.set("URL:" + url);
                context.write(outputKey, ONE);

                outputKey.set("STATUS:" + status);
                context.write(outputKey, ONE);

                outputKey.set("IP:" + ip);
                context.write(outputKey, ONE);
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: WebLogAnalysis <input path> <output path>"
            );
            System.exit(2);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Web Server Log Analysis");

        job.setJarByClass(WebLogAnalysis.class);
        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
