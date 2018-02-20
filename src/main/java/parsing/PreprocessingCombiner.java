package parsing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessingCombiner extends Reducer<Text, Text, Text, Text> {
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	Text valueText = new Text();
	
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
		valueText.set(format.format(latestDate) + "###" + latestOutLinks);
		context.write(key, valueText);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
