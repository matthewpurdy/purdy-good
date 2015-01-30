package purdygood.accumulo.mapred;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExportTableMapper extends Mapper<Key, Value, NullWritable, Text> {
	private static NullWritable NULL_WRITE = NullWritable.get();
	
	private StringBuilder keyValueString = new StringBuilder(128);
	private Text text                    = new Text();
	
	@Override
    public void map(Key key, Value value, Context context) throws IOException, InterruptedException {
		keyValueString.append(key.getRow()).append(",");
		keyValueString.append(key.getColumnFamily()).append(",");
		keyValueString.append(key.getColumnQualifier()).append(",");
		keyValueString.append(key.getColumnVisibility()).append(",");
		keyValueString.append(key.getTimestamp()).append(",");
		keyValueString.append(value);
		
		text.set(keyValueString.toString());
		context.write(NULL_WRITE, text);
		keyValueString.setLength(0);
	}
}
