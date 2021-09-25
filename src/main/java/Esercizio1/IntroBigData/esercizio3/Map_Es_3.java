package Esercizio1.IntroBigData.esercizio3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import generics.Separator;

/**
 * Map job 3
 * 
 * @author Nicola Carlucci
 *
 */
public class Map_Es_3 extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * List all couples
	 */
	List<String> allCouples = new ArrayList<String>();

	/**
	 * User for summary row file
	 */
	int count = 0;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		count++;
		String lineString = value.toString();
		// remove date
		String lineStringNoDate = removeDate(lineString, Separator.COMMA);
		if (lineStringNoDate != null && lineStringNoDate != "") {
			// get all couples
			List<String> listCoupleline = generateCouples(lineStringNoDate);
			if (listCoupleline != null) {
				for (String couple : listCoupleline) {
					String association = supportAssociation(lineStringNoDate, couple, Separator.COMMA);
					if (association != null) {
						association = association + Separator.SEMICOLON + "i:" + String.valueOf(count);
						context.write(new Text(couple), new Text(association));
					} else {
						String confident = confidentAssociation(lineStringNoDate, couple, Separator.COMMA);
						if (confident != null) {
							confident = confident + Separator.SEMICOLON + "i:" + String.valueOf(count);
							context.write(new Text(couple), new Text(confident));
						}
					}
				} // for

				// add rule to all couples if not exists in lineString
				for (String coupleold : allCouples) {
					if (!listCoupleline.contains(coupleold)) {
						String association = supportAssociation(lineStringNoDate, coupleold, Separator.COMMA);
						if (association != null) {
							// supportAssociation:1;confidentAssociation:0
							// add i: total
							association = association + Separator.SEMICOLON + "i:" + String.valueOf(count);
							context.write(new Text(coupleold), new Text(association));
						} else {
							String confident = confidentAssociation(lineStringNoDate, coupleold, Separator.COMMA);
							if (confident != null) {
								confident = confident + Separator.SEMICOLON + "i:" + String.valueOf(count);
								context.write(new Text(coupleold), new Text(confident));
							}
						}
					}
				} // for

				// and update list all couples
				updateAllCouples(listCoupleline);
			}

		}

	}

	/**
	 * this metod return null if support Association rule is not present, else
	 * return row example : supportAssociation:1;confidentAssociation:0
	 * 
	 * @param linString       string to check rule
	 * @param couple          couple to check rule
	 * @param separatorString
	 * @return null or supportAssociation:1;confidentAssociation:0
	 */
	private String supportAssociation(String linString, String couple, String separatorString) {
		try {
			String coupleArray[] = couple.split(Separator.COMMA);
			String p1 = coupleArray[0];
			String p2 = coupleArray[1];
			String[] lineStringArray = linString.split(separatorString);
			boolean supportAssociation = false;
			for (int i = 0; i < lineStringArray.length; i++) {
				String p1_Line = lineStringArray[i];
				if (p1_Line.equalsIgnoreCase(p1)) {
					if (i < lineStringArray.length - 1) {
						String p2_line = lineStringArray[i + 1];
						if (p2_line.equalsIgnoreCase(p2)) {
							supportAssociation = true;
						}
					}
				}
			}
			if (supportAssociation) {
				String contextWrite = Separator.SUPPORT_ASSOCIATION + "1" + Separator.SEMICOLON
						+ Separator.CONFIDENT_ASSOCIATION + "0";
				return contextWrite;
			} else {
				return null;
			}
		} catch (Exception exception) {
			return null;
		}
	}

	/**
	 * return null if do any exception, else string example:
	 * supportAssociation:0;confidentAssociation:0 or
	 * supportAssociation:0;confidentAssociation:1
	 * 
	 * @param linString       string to check rule
	 * @param couple          couple to check rule
	 * @param separatorString
	 * @return null or supportAssociation:0;confidentAssociation:0 or
	 *         upportAssociation:0;confidentAssociation:1
	 */
	private String confidentAssociation(String linString, String couple, String separatorString) {
		try {
			String coupleArray[] = couple.split(Separator.COMMA);
			String p1 = coupleArray[0];
			String p2 = coupleArray[1];
			String[] lineStringArray = linString.split(separatorString);
			int confidentAssociation = 0;
			for (int i = 0; i < lineStringArray.length; i++) {
				String p1_Line = lineStringArray[i];
				if (p1_Line.equalsIgnoreCase(p1)) {
					for (int j = 0; j < lineStringArray.length; j++) {
						String p2_Line = lineStringArray[j];
						if (p2_Line.equalsIgnoreCase(p2)) {
							confidentAssociation = 1;
							break;
						}
					}
				}
			}

			String contextWrite = Separator.SUPPORT_ASSOCIATION + "0" + Separator.SEMICOLON
					+ Separator.CONFIDENT_ASSOCIATION + confidentAssociation;
			return contextWrite;

		} catch (Exception exception) {
			return null;
		}
	}

	/**
	 * Update list allCouples
	 * 
	 * @param newListCouples new list of couples
	 */
	private void updateAllCouples(List<String> newListCouples) {
		for (String newCouple : newListCouples) {
			if (!allCouples.contains(newCouple)) {
				allCouples.add(newCouple);
			}
		}
	}

	/**
	 * Generate all couples from lineString Example lineString a,b,c to (a,b) (a,c)
	 * (b,c)
	 * 
	 * @param lineString
	 * @return List all couples
	 */
	private List<String> generateCouples(String lineString) {
		List<String> couples = new ArrayList<String>();
		try {
			String[] lineStringArray = lineString.split(Separator.COMMA);
			// generate all couples
			for (int i = 0; i < lineStringArray.length - 1; i++) {
				for (int j = i + 1; j < lineStringArray.length; j++) {
					couples.add(lineStringArray[i] + Separator.COMMA + lineStringArray[j]);
					couples.add(lineStringArray[j] + Separator.COMMA + lineStringArray[i]);
				}
			}
		} catch (Exception exception) {
			exception.printStackTrace();
			couples = null;
		}
		return couples;
	}

	/**
	 * Remove date from string example 2015-5-28,uova,dolce,melanzane to uova,
	 * dolce, melanzana
	 * 
	 * @param lineString      string to split
	 * @param separatorString char separator
	 * @return string no date
	 */
	private String removeDate(String lineString, String separatorString) {
		try {
			String[] lineStringArray = lineString.split(separatorString);
			String lineStringNoDate = "";
			for (int i = 1; i < lineStringArray.length; i++) {
				lineStringNoDate = lineStringNoDate + (i != 1 ? Separator.COMMA : "") + lineStringArray[i];
			}
			return lineStringNoDate;
		} catch (Exception exception) {
			exception.printStackTrace();
			return null;
		}
	}

}
