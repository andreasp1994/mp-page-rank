package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import formatoutput.FormatOutputMapper;
import pagerank.PageRankMapper;
import pagerank.PageRankReducer;
import parsing.PreprocessingMapper;
import parsing.PreprocessingReducer;


public class MRPageRank extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		boolean succeeded = this.runParsingJob(args[0], args[1]);
		if(!succeeded) {
			System.out.println("ERROR");
			return 1;
		}
//		boolean succeeded = true;
		System.out.println("Parsing Job Finished!");
		int numLoops = Integer.valueOf(args[2]);
		for (int i = 0; i < numLoops; i++) {
			succeeded = this.runPageRankJob(args[1], i);
			if (!succeeded) {
				System.out.println("ERROR");
				return 1;
			}
			System.out.println("PageRank Job " + i + " Finished!");
		}
		succeeded = this.runFormatOutputJob(args[1], numLoops-1);
		if(!succeeded) {
			System.out.println("ERROR");
			return 1;
		}
		System.out.println("FormatOutput Job Finished!");
	    return 0;
	}
	
	public boolean runParsingJob(String inputPath, String outputPath) throws Exception {
		Configuration conf1 = getConf();
		conf1.set("textinputformat.record.delimiter", "\n\n");
		conf1.setBoolean("mapred.compress.map.output", true);
		Job job = Job.getInstance(conf1, "PageRank_Preprocessing");
		job.setJarByClass(MRPageRank.class);
		job.setMapperClass(PreprocessingMapper.class);
//		job.setCombinerClass(PreprocessingReducer.class);
		job.setReducerClass(PreprocessingReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/parsing"));
	    return job.waitForCompletion(true);
	}

	public boolean runPageRankJob(String inputPath, int iter) throws Exception {
		Configuration conf2 = getConf();
		conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		conf2.setBoolean("mapred.compress.map.output", true);
	    Job job2 = Job.getInstance(conf2, "PageRank_Calculate");
	    job2.setJarByClass(MRPageRank.class);
		job2.setMapperClass(PageRankMapper.class);
		job2.setReducerClass(PageRankReducer.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		if(iter == 0) {
			FileInputFormat.setInputPaths(job2, new Path(inputPath + "/parsing"));
		} else {
			FileInputFormat.setInputPaths(job2, new Path(inputPath + "/pagerank/iter" + (iter-1)));
		}
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
	    FileOutputFormat.setOutputPath(job2, new Path(inputPath + "/pagerank/iter" + iter));
	    return job2.waitForCompletion(true);
	}
	
	public boolean runFormatOutputJob(String inputPath, int lastIter) throws Exception {
		Configuration conf2 = getConf();
		conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		conf2.setBoolean("mapred.compress.map.output", true);
	    Job job2 = Job.getInstance(conf2, "PageRank_FormatOutput");
	    job2.setJarByClass(MRPageRank.class);
		job2.setMapperClass(FormatOutputMapper.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(inputPath + "/pagerank/iter" + (lastIter)));
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
	    FileOutputFormat.setOutputPath(job2, new Path(inputPath + "/output"));
	    return job2.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MRPageRank(), args));
	}
}
