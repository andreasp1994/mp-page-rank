package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankCombiner extends Reducer<Text, Text, Text, Text> {
	Text valueText = new Text();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double partialContribution = 0;
		for (Text value : values) {
			if (value.toString().startsWith("$LINKSOUT$")) {
				context.write(key, value);
				continue;
			}
			double contribution = Double.valueOf(value.toString());
			partialContribution += contribution;	
		}
		valueText.set(String.valueOf(partialContribution));
		context.write(key, valueText);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	} 
}
