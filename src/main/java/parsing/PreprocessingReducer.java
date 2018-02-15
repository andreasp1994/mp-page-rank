package parsing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {
	SimpleDateFormat format = new SimpleDateFormat(
		    "yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Date revisionDate = new Date(0);
		String revisionDateStr;
		Date latestDate = new Date(0);
		String latestOutLinks = "";
		String[] valueFields;
		for (Text value: values) {
			try {	// Surround with try and catch in case date is not formatted correctly.
				valueFields = value.toString().split("###");
				revisionDateStr = valueFields[0];
				revisionDate = format.parse(revisionDateStr);
				if (revisionDate.compareTo(latestDate) > 0) {
					latestDate = revisionDate;
					latestOutLinks = valueFields[1];
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		Set<String> parsedLinks = new HashSet<String>();
		for(String outLink : latestOutLinks.split("\\s+")) {
			if (outLink.equals("MAIN")) continue;
			if (outLink.indexOf(" ") > 0) {
				System.out.println("AA");
			}
			parsedLinks.add(outLink);
		}
		Text pageRankAndLinks = new Text("1.0###" + StringUtils.join(parsedLinks, " ").trim());
		context.write(key, pageRankAndLinks);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
