package ECG.IntroBigData.job3;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import generics.ECGValue;
import generics.Separator;

public class Map_Job3_ECG extends Mapper<LongWritable, Text, Text, Text>{

	double prec = -1;
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ECGValue ecgValue = getECGValue(value.toString(), Separator.COMMA);
		if (prec == -1) {
			//first cicle
			prec = ecgValue.getVolt();
		}
		if((ecgValue.getVolt() >= 0.95 && prec <0.95)) {
			//up
			Text keyText = new Text("tiktok");
			Text typeText = new Text("UP");
			prec = ecgValue.getVolt();
			context.write(keyText, typeText);
		}else {
			if(ecgValue.getVolt() < 0.95 && prec >=0.95) {
				//down
				prec = ecgValue.getVolt();
				Text keyText = new Text("tiktok");
				Text typeText = new Text("DOWN");
				context.write(keyText, typeText);
			}else {
				//no write
			}
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
