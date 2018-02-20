package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	Text keyArticleTitle = new Text();
	Text valueText = new Text();
	
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
				keyArticleTitle.set(article);
				double contribution = Double.valueOf(pageRank)/linksOutLength;
				valueText.set(String.valueOf(contribution));
				context.write(keyArticleTitle, valueText);
			}
			valueText.set("$LINKSOUT$" + linksOut);
			context.write(key, valueText);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
