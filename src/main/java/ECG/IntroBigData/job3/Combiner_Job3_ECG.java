package ECG.IntroBigData.job3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner_Job3_ECG extends Reducer<Text, IntWritable, Text, Text> {

	private final static IntWritable ONE = new IntWritable(1);

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, Text>.Context arg2) throws IOException, InterruptedException {
		
		String statusPrec = "DOWN";
		for(Object object : arg1) {
			String value = object.toString();
			if(value.equalsIgnoreCase("UP") && statusPrec.equalsIgnoreCase("DOWN")) {
				statusPrec = value;
			}else {
				if(value.equalsIgnoreCase("DOWN") && statusPrec.equalsIgnoreCase("UP")){
					Text keyText = new Text("tiktok");
					Text valueText = new Text("1");
					IntWritable ONE = new IntWritable(1);
					arg2.write(keyText, valueText);
				}
			}
		}
		
	}
	
	
}
