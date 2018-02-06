package pagerank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import writables.PageRankWritable;

public class PageRankReducer extends Reducer<Text, PageRankWritable, Text, PageRankWritable> {
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		// ...
		
	}
	
	@Override
	protected void reduce(Text key, Iterable<PageRankWritable> values, Context context) throws IOException, InterruptedException {
		double pageRankSum = 0;
		PageRankWritable out = null;
		Set<String> linksOut = new HashSet<String>();
		for(PageRankWritable pageRank : values) {
			if (pageRank.getLinksOut().toString().equals("")) {
				pageRankSum += pageRank.getPageRank().get();
			} else {
				out = pageRank;
			}
		}
		out.setPageRank(new DoubleWritable(0.15+0.85*pageRankSum));
		context.write(key, out);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ...
		super.cleanup(context);
	} 
}
