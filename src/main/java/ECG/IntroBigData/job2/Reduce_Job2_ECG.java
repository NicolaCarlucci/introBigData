package ECG.IntroBigData.job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import generics.CalculationsResult;
import generics.ImplUtilityECGValues;
import generics.Separator;

public class Reduce_Job2_ECG extends Reducer<Object, Object, Object, Object>{

	@Override
	protected void reduce(Object arg0, Iterable<Object> arg1, Reducer<Object, Object, Object, Object>.Context arg2)
			throws IOException, InterruptedException {
		
		List<Double> values = new ArrayList<Double>();
		ImplUtilityECGValues implUtilityECGValues = new ImplUtilityECGValues ();
		for(Object object : arg1) {
			values.add(Double.parseDouble(object.toString()));
		}
		
		if(!values.isEmpty()) {
			double[] valueArray = values.stream().mapToDouble(Double::doubleValue).toArray(); //via method reference
			CalculationsResult calculationsResult = implUtilityECGValues.calculateCalculationsResult(valueArray);
			Text keyText = new Text("Job2 ECG ->");
			Text valueText = new Text(" average " + Separator.DOUBLE_POINTS + calculationsResult.getAverage()+ " variance"+ Separator.DOUBLE_POINTS + calculationsResult.getVariance());
			arg2.write(keyText, valueText);
		}
		
		
	}

	
	
}
