package Esercizio1.IntroBigData.esercizio1.part2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import generics.LineMap;
import generics.Separator;
import generics.UtilityLineMap;

/**
 * Reduce class job1 part 2
 * 
 * @author Nicola Carlucci
 *
 */
public class Reduce_Es1_part2 extends Reducer<Object, Object, Object, Object> implements UtilityLineMap {

	@Override
	protected void reduce(Object arg0, Iterable<Object> arg1, Reducer<Object, Object, Object, Object>.Context arg2)
			throws IOException, InterruptedException {

		List<LineMap> listLineMap = new ArrayList<>();

		List<LineMap> resultLineMapList = new ArrayList<>();

		for (Object object : arg1) {
			LineMap lineMap = getLineMap(object.toString(), Separator.SPACE);
			if (lineMap != null) {
				// add element to key
				// example 2015-01: pane 852
				// 2015-01: latte 753
				listLineMap.add(lineMap);
			}
		}

		// get first 5 elements
		if (listLineMap.size() != 0) {
			listLineMap = getLineMapOrder(listLineMap);
			resultLineMapList = listLineMap.subList(0, 5);
		}

		Text dateText = new Text(arg0.toString()+";");
		String resultString = "";
		for (LineMap result : resultLineMapList) {
			// result
			// example
			// 2015-01: pane 852, latte 753, carne 544, vino 501, pesce 488
			resultString = resultString + result.getProduct() +";"+  Separator.SPACE +String.valueOf(result.getIntValue()) +";"+  Separator.SPACE;
		}

		if (!resultString.equalsIgnoreCase("")) {
			Text valueText = new Text(resultString);
			arg2.write(dateText, valueText);
		}

	}

	@Override
	public LineMap getLineMap(String lineString, String separatorString) {
		LineMap lineMap = new LineMap();
		try {
			lineString = lineString.replace("[", "").replace("]", "");
			String[] allString = lineString.split(separatorString);
			lineMap.setProduct(allString[0]);
			lineMap.setIntValue(Integer.parseInt(allString[1]));
		} catch (Exception exception) {
			lineMap = null;
		}
		return lineMap;
	}

	/**
	 * Order list lineMap objects from intValue
	 * 
	 * @param listLineMap list to order
	 * @return list lineMap order
	 */
	private List<LineMap> getLineMapOrder(List<LineMap> listLineMap) {
		return listLineMap.stream().sorted(Comparator.comparing(LineMap::getIntValue).reversed())
				.collect(Collectors.toList());
	}

}
