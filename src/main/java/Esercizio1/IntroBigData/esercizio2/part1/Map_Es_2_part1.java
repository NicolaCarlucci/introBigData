package Esercizio1.IntroBigData.esercizio2.part1;

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
 * Map class job2 part 1
 * 
 * @author Nicola Carlucci
 *
 */
public class Map_Es_2_part1 extends Mapper<LongWritable, Text, Text, IntWritable> implements UtilityLineMap {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		List<LineMap> lineMapList = getListLineMap(value.toString(), Separator.COMMA);
		if (lineMapList != null && lineMapList.size() != 0) {
			for (LineMap lineMap : lineMapList) {
				Text textKey = new Text(lineMap.getDataString() + Separator.SPACE + lineMap.getProduct());
				context.write(textKey, new IntWritable(lineMap.getIntValue()));
				// 2015-05 acqua ->32
				// 2015-05 birra ->64
			}
		}

	}

	/**
	 * Get list lineMap objects from lineString example
	 * 2015-5-28,uova/3,dolce/9,melanzane/2,formaggio/8 to 2015-05 -> (uova:3)
	 * (dolce:9)......
	 * 
	 * @param lineString      line to format
	 * @param separatorString
	 * @return list lineMap
	 */
	private List<LineMap> getListLineMap(String lineString, String separatorString) {
		List<LineMap> lineMapList = new ArrayList<LineMap>();
		// split line , example :
		// 2015-5-28,uova/3,dolce/9,melanzane/2,formaggio/8,piadina/10,birra/4,parmigiano/13
		String[] lineStringArray = lineString.split(separatorString);

		// remove day to date
		String[] dateNoDayStringArray = lineStringArray[0].split(Separator.DASH);
		String dateNoDayString = dateNoDayStringArray[0] + Separator.DASH + dateNoDayStringArray[1];

		for (int i = 1; i < lineStringArray.length; i++) {
			// no forEach because not use lineStringArray[0], is date
			LineMap lineMap = getLineMap(lineStringArray[i], Separator.SLASH);
			if (lineMap != null) {
				lineMap.setDataString(dateNoDayString);
				lineMapList.add(lineMap);
			}
		}
		return lineMapList;

	}

	@Override
	public LineMap getLineMap(String lineString, String separatorString) {
		LineMap lineMap = new LineMap();
		try {
			String[] lineStringArray = lineString.split(separatorString);
			lineMap.setProduct(lineStringArray[0]);
			lineMap.setIntValue(Integer.parseInt(lineStringArray[1]));
		} catch (Exception exception) {
			exception.printStackTrace();
			lineMap = null;
		}
		return lineMap;
	}

}
