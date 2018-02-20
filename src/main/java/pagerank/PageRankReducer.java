package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	Text valueText = new Text();
	
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
			double contribution = Double.valueOf(value.toString());
			pageRankSum += contribution;	
		}
		double finalPageRank = 0.15+0.85*pageRankSum;
		valueText.set(String.valueOf(finalPageRank) + "###" + linksOut);
		context.write(key, valueText);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	} 
}
