package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class PageRankWritable {
	private DoubleWritable pageRank;
    private Text linksOut;
   
    public PageRankWritable() {
        pageRank = new DoubleWritable(1);
        linksOut = new Text(); 
    }
 
    public PageRankWritable(DoubleWritable pageRank, Text linksOut) {
        this.pageRank = pageRank;
        this.linksOut = linksOut;
    }
 
    public DoubleWritable getPageRank() {
        return pageRank;
    }
 
    public Text getLinksOut() {
        return linksOut;
    }
 
    public void setPageRank(DoubleWritable pageRank) {
        this.pageRank = pageRank;
    }
 
    public void setLinksOut(Text linksOut) {
        this.linksOut = linksOut;
    }
 
    public void readFields(DataInput in) throws IOException {
        pageRank.readFields(in);
        linksOut.readFields(in);
    }
 
    public void write(DataOutput out) throws IOException {
        pageRank.write(out);
        linksOut.write(out);
    }
 
    @Override
    public String toString() {
        return pageRank.toString() + "\t" + linksOut.toString();
    }
}
