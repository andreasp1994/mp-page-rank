package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;


public class MySimpleMapReduceJob extends Configured implements Tool {

	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println("VALUE: " + line);
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}
		
		// The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		// Make sure that the output key/value classes also match those set in your job's configuration (see below).
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value: values)
				sum += value.get();
			context.write(key, new IntWritable(sum));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCount-v1");
		job.setJarByClass(MySimpleMapReduceJob.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MySimpleMapReduceJob(), args));
	}
}
