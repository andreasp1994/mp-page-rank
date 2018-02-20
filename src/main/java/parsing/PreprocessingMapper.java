package parsing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text>  {
	Text valueText = new Text();
	Text keyText = new Text();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String revision = value.toString();
		String[] lines = revision.split(System.getProperty("line.separator"));
		String[] articleFields = lines[0].split(" ");
		keyText.set(articleFields[3]);
		String revisionDateStr = articleFields[4];
		String linksOut = lines[3];
		valueText.set(revisionDateStr + "###" + linksOut);
		context.write(keyText, valueText);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}

}


