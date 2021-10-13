package ECG.IntroBigData.job3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import generics.Separator;

public class Reduce_Job3_ECG extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, Text>.Context arg2) throws IOException, InterruptedException {
		
		int sum = 0;
		for(Object object : arg1) {
			sum = sum + Integer.parseInt(object.toString());
		}
		Text keyText = new Text("ECG counts " + Separator.DOUBLE_POINTS);
		//IntWritable SUM = new IntWritable(sum);
		arg2.write(keyText, new Text(String.valueOf(sum)));
	}

}	
	

