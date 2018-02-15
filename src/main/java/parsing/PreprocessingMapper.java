package parsing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text>  {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String revision = value.toString();
		String[] lines = revision.split(System.getProperty("line.separator"));
		String[] articleFields = lines[0].split(" ");
		Text articleTitle = new Text(articleFields[3]);
		String revisionDateStr = articleFields[4];
		String linksOut = lines[3];
		context.write(new Text(articleTitle), new Text(revisionDateStr + "###" + linksOut));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}

}


