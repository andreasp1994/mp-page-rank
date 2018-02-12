package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(Text key, Text pageRankAndLinks, Context context) throws IOException, InterruptedException {
		String[] splittedValues = pageRankAndLinks.toString().split("###");
		if (splittedValues.length > 1) {	// Has outgoing links.
			String pageRank = splittedValues[0];
			String linksOut = splittedValues[1];
			String[] linksOutArr = linksOut.split(" ");
			int linksOutLength = linksOutArr.length;
			for(String article : linksOutArr) {
				context.write(new Text(article), new Text(pageRank + "###" + linksOutLength + "###" + key.toString()));
			}
			context.write(key, new Text("$LINKSOUT$" + linksOut));
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
