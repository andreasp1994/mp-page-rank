package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double pageRankSum = 0;
		String linksOut = "";
		for (Text value : values) {
			if (value.toString().startsWith("$LINKSOUT$")) {
				linksOut = value.toString().substring("$LINKSOUT$".length());
				continue;
			}
			double pageRank = Double.valueOf(value.toString().split("###")[0]);
			int lengthOfLinks = Integer.valueOf(value.toString().split("###")[1]);
			pageRankSum += (pageRank/lengthOfLinks);	
		}
		double finalPageRank = 0.15+0.85*pageRankSum;
		context.write(key, new Text( String.valueOf(finalPageRank) + "###" + linksOut));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	} 
}
