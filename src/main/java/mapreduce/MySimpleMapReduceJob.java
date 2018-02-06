package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import pagerank.PageRankMapper;
import pagerank.PageRankReducer;
import parsing.PreprocessingMapper;
import parsing.PreprocessingReducer;
import writables.PageRankWritable;

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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;


public class MySimpleMapReduceJob extends Configured implements Tool {

	
	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		JobControl jobControl = new JobControl("jobChain");
		
//		Configuration conf1 = getConf();
//		conf1.set("textinputformat.record.delimiter", "\n\n");
//		Job job = Job.getInstance(conf1, "PageRank_Preprocessing");
//		job.setJarByClass(MySimpleMapReduceJob.class);
//		job.setMapperClass(PreprocessingMapper.class);
//		job.setReducerClass(PreprocessingReducer.class);
//		job.setInputFormatClass(TextInputFormat.class);
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
//		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/parsed"));
//		
//		ControlledJob controlledJob1 = new ControlledJob(conf1);
//	    controlledJob1.setJob(job);
//	    jobControl.addJob(controlledJob1);
//		job.waitForComplet/ion(true);
		
		Configuration conf2 = getConf();
	    Job job2 = Job.getInstance(conf2, "PageRank_Calculate");
	    job2.setJarByClass(MySimpleMapReduceJob.class);
		job2.setMapperClass(PageRankMapper.class);
		job2.setReducerClass(PageRankReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1] + "/parsded/part-r-00000"));
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(PageRankWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
	    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
	    return (job2.waitForCompletion(true) ? 0 : 1);
//	    ControlledJob controlledJob2 = new ControlledJob(conf2);
//	    controlledJob2.setJob(job2);
//	    jobControl.addJob(controlledJob2);
//	    
//	    controlledJob2.addDependingJob(controlledJob1);
//	    
//	    Thread jobControlThread = new Thread(jobControl);
//	    jobControlThread.start();
//	    
//	    while (!jobControl.allFinished()) {
//	        System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
//	        System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
//	        System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
//	        System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
//	        System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
//	    try {
//	        Thread.sleep(5000);
//	        } catch (Exception e) {
//
//	        }
//
//	      } 
//	      System.exit(0);  
////	        
//		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	// Your main Driver method. Note: everything in this method runs locally at the client.
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MySimpleMapReduceJob(), args));
	}
}
