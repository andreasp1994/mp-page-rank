package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import writables.PageRankWritable;

public class PageRankMapper extends Mapper<Text, PageRankWritable, Text, PageRankWritable> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(Text key, PageRankWritable pageRank, Context context) throws IOException, InterruptedException {
		String[] linksOut = pageRank.getLinksOut().toString().split(" ");
		int linksOutLength = linksOut.length;
		
		double pr = pageRank.getPageRank().get()/linksOutLength;
		context.write(key, pageRank);
		for(String link : linksOut) {
			context.write(new Text(link), new PageRankWritable(new DoubleWritable(pr), new Text("")));
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ...
		super.cleanup(context);
	}
}
