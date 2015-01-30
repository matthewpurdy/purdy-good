package purdygood.accumulo.mapred;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImportTableMapper extends Mapper<LongWritable, Text, Text, Mutation> {
	private Text text = new Text();
	
	private static Text setText(Text text, String string) {
		text.set(string);
		return text;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context output) throws IOException {
		String[] data = value.toString().split(",");
		
		Mutation mutation = new Mutation(setText(text, data[0]));
		mutation.put(setText(text, data[1]), setText(text, data[2]), new ColumnVisibility(data[3]), new Value(value.getBytes()));
		try { output.write(null, mutation); } catch (InterruptedException e) { e.printStackTrace(); }
	}
}
