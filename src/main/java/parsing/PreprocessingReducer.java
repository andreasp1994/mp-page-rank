package parsing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> linksOut = new HashSet<String>();
		for (Text value: values) {
			linksOut.add(value.toString());
		}
		Text pageRankAndLinks = new Text("1###" + StringUtils.join(linksOut, " ").trim());
		context.write(key, pageRankAndLinks);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ...
		super.cleanup(context);
	}
}
