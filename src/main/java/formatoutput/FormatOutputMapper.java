package formatoutput;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FormatOutputMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(Text key, Text pageRankAndLinks, Context context) throws IOException, InterruptedException {
		String pageRank = pageRankAndLinks.toString().split("###")[0];		
		context.write(key, new Text(pageRank));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}

}
