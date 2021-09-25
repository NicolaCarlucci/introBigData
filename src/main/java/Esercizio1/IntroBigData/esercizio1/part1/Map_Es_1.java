package Esercizio1.IntroBigData.esercizio1.part1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import generics.LineMap;
import generics.Separator;
import generics.UtilityLineMap;

/**
 * Map Esercizio 1 part 1
 * 
 * @author Nicola Carlucci
 *
 */
public class Map_Es_1 extends Mapper<LongWritable, Text, Text, IntWritable> implements UtilityLineMap{
	
	private static final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		LineMap lineMap = getLineMap(value.toString(),Separator.COMMA);
		if(lineMap!=null) {
			Text text = new Text();
			for(String product : lineMap.getValue()) {
				text.set(lineMap.getDataString()+" "+ product);
				context.write(text, ONE);
				
				// map job, insert in to context key, product 1
				// example---------------------->2015-01 birra 1
			}
		}
	}
	
	

	/**
	 * Create lineMap object from string
	 * 
	 */
	@Override
	public LineMap getLineMap(String lineString, String separatorString) {
		LineMap lineMap = new LineMap();
		try {
			String[] arrayValue = lineString.split(separatorString);
			// date
			String fullDateString = arrayValue[0];
			
			//remove day to date
			String[] dateNoDayStringArray = fullDateString.split(Separator.DASH);
			String dateNoDayString = dateNoDayStringArray[0]+Separator.DASH+dateNoDayStringArray[1];
			
			lineMap.setDataString(dateNoDayString);
			
			//re-compose value
			List<String> value = new ArrayList<String>();
			for(int i=1;i<arrayValue.length;i++) {
				value.add(arrayValue[i]);
			}
			lineMap.setValue(value);
			// return data- list product
			//example 2015-01 , List<acqua,birra,pane......>
		}catch(Exception exception) {
			exception.printStackTrace();
			lineMap = null;
		}
		
		return lineMap;
	}

}
