package ECG.IntroBigData.job1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import generics.CalculationsResult;
import generics.ECGValue;
import generics.ImplUtilityECGValues;
import generics.Separator;

public class Map_Job1_ECG extends Mapper<LongWritable, Text, Text, Text>{

	List<Double> values = new ArrayList<Double>();
	ImplUtilityECGValues implUtilityECGValues = new ImplUtilityECGValues ();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ECGValue ecgValue = getECGValue(value.toString(), Separator.COMMA);
		values.add(ecgValue.getVolt());
		/*
		if(ecgValue.getVolt() >= 0.95) {
			double[] valueArray = values.stream().mapToDouble(Double::doubleValue).toArray(); //via method reference
			CalculationsResult calculationsResult = implUtilityECGValues.calculateCalculationsResult(valueArray);
			if(calculationsResult!=null) {
				Text timeText = new Text(String.valueOf(ecgValue.getTime()));
				Text standardDeviationText = new Text(" standard deviation "+ Separator.DOUBLE_POINTS + String.valueOf(calculationsResult.getStandardDeviation())+ " value volt "+Separator.DOUBLE_POINTS + String.valueOf(ecgValue.getVolt()));
				context.write(timeText, standardDeviationText);

			}
			
		}
		*/
		// per il grafici
		if(ecgValue.getVolt() >= 0.85) {
			double[] valueArray = values.stream().mapToDouble(Double::doubleValue).toArray(); //via method reference
			CalculationsResult calculationsResult = implUtilityECGValues.calculateCalculationsResult(valueArray);
			if(calculationsResult!=null) {
				Text timeText = new Text(String.valueOf(ecgValue.getTime()));
				Text standardDeviationText = new Text("standarDeviation"+ Separator.COMMA + String.valueOf(calculationsResult.getStandardDeviation())+ "valueVolt"+Separator.COMMA + String.valueOf(ecgValue.getVolt()));
				context.write(timeText, standardDeviationText);

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
