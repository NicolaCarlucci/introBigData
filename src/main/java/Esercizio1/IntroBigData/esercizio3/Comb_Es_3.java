package Esercizio1.IntroBigData.esercizio3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import generics.Association;
import generics.Separator;

/**
 * Combiner class job3
 * 
 * @author Nicola Carlucci
 *
 */
public class Comb_Es_3 extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Reducer<Text, IntWritable, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		Association associationFinal = new Association();
		double numSupport = 0;
		double numConfident = 0;

		// add total
		for (Object object : arg1) {
			Association association = getAssociation(object.toString(), Separator.SEMICOLON, Separator.DOUBLE_POINTS);
			if (association != null) {
				// set total
				if (associationFinal.getTotal() < association.getTotal()) {
					associationFinal.setTotal(association.getTotal());
				}
				// increment support association
				numSupport = numSupport + association.getNumSupportAssociation();

				// increment confident association
				numConfident = numConfident + association.getNumConfidentAssociation();

			}
		}
		// write context
		arg2.write(arg0,
				new Text(Separator.SUPPORT_ASSOCIATION + numSupport + Separator.SEMICOLON
						+ Separator.CONFIDENT_ASSOCIATION + numConfident + Separator.SEMICOLON + "i"
						+ Separator.DOUBLE_POINTS + associationFinal.getTotal()));

	}

	/**
	 * Read lineString and get object type Association
	 * 
	 * @param lineString               string to format Object
	 * @param separatorString          char separator properties
	 * @param separatorStringProperies char separator value properties
	 * @return Association or null
	 */
	private Association getAssociation(String lineString, String separatorString, String separatorStringProperies) {
		Association association = new Association();
		try {
			String[] lineStringArray = lineString.split(separatorString);
			for (String properties : lineStringArray) {
				String[] propertiesArray = properties.split(separatorStringProperies);
				String keyProperties = propertiesArray[0] + Separator.DOUBLE_POINTS;
				switch (keyProperties) {
				case Separator.SUPPORT_ASSOCIATION:
					association.setNumSupportAssociation(Double.parseDouble(propertiesArray[1]));
					break;
				case Separator.CONFIDENT_ASSOCIATION:
					association.setNumConfidentAssociation(Double.parseDouble(propertiesArray[1]));
					break;
				case "i:":
					association.setTotal(Integer.parseInt(propertiesArray[1]));
				}
			}
		} catch (Exception exception) {
			association = null;
			exception.printStackTrace();
		}
		return association;
	}

}
