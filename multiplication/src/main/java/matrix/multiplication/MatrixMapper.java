package matrix.multiplication;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int m = Integer.parseInt(conf.get("m"));
		int p = Integer.parseInt(conf.get("p"));
		String line = value.toString();
		String[] indicesAndValue = line.split(",");
		Text outputKey = new Text();
		Text outputValue = new Text();
		if (indicesAndValue[0].equals("M")) {
			{
				for (int k = 0; k < p; k++)
					outputKey.set(indicesAndValue[1] + "," + k);
				outputValue.set("M," + indicesAndValue[2] + "," + indicesAndValue[3]);
				context.write(outputKey, outputValue);
			}
		} else {
			for (int i = 0; i < m; i++) {
				outputKey.set(i + "," + indicesAndValue[2]);
				outputValue.set("N," + indicesAndValue[1] + "," + indicesAndValue[3]);
				context.write(outputKey, outputValue);
			}
		}
	}
}
