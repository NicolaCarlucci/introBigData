package ECG.IntroBigData.job2;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import generics.ECGValue;
import generics.Separator;

public class Map_Job2_ECG extends Mapper<LongWritable, Text, Text, Text>{

	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ECGValue ecgValue = getECGValue(value.toString(), Separator.COMMA);
	
		if(ecgValue!=null) {
			Text keyText = new Text("key");
			Text voltText = new Text(String.valueOf(ecgValue.getVolt()));
			context.write(keyText, voltText);
		}
		
	}

	private ECGValue getECGValue(String input, String separatorString) {
		ECGValue ecgValue = new ECGValue();
		try {
			String[] inputString  = input.split(separatorString);
			//double time = inputString[0].replace(0, 0)
			ecgValue.setTime(Double.parseDouble(inputString[0]));
			ecgValue.setVolt(Double.parseDouble(inputString[1]));
		}catch(Exception exception) {
			exception.printStackTrace();
			ecgValue = null;
		}
		return ecgValue;
	}

	
}

