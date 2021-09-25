package Esercizio1.IntroBigData.esercizio2.part2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import generics.LineMap;
import generics.Separator;

public class Reduce_Es_2_part2 extends Reducer<Object, Object, Object, Object> {
	

	@Override
	protected void reduce(Object arg0, Iterable<Object> arg1, Reducer<Object, Object, Object, Object>.Context arg2)
			throws IOException, InterruptedException {
		LineMap lineMap = new LineMap();
		lineMap.setProduct(arg0.toString());
		List<String> value = new ArrayList<String>();
		for(Object object : arg1) {
			value.add(object.toString());
		}
		lineMap.setValue(value);
		
		if(lineMap.getValue().size()!=0) {
			String result="";
			for(String dateAndAmount : lineMap.getValue()) {
				result = result + Separator.SPACE + dateAndAmount;
			}
			arg2.write(new Text(lineMap.getProduct()), new Text(result));
		}
		
	}
	
	

}
