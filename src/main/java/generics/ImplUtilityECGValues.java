package generics;


public class ImplUtilityECGValues implements UtilityECGValues{

	@Override
	public CalculationsResult calculateCalculationsResult(double[] values) {
		CalculationsResult calculationsResult = new CalculationsResult();
		double average = getAverage(values);
		try {
			double sum = 0;
			int valuesCount = values.length;
			for(int i = 0; i<valuesCount; i++) {
				double xiValue = (values[i] -average) * (values[i] -average);
				sum = sum + xiValue;
			}
			double variance = sum / (valuesCount-1);
			double standardDeviation = Math.sqrt(variance);
			calculationsResult.setAverage(average);
			calculationsResult.setVariance(variance);
			calculationsResult.setStandardDeviation(standardDeviation);
		}catch(Exception exception) {
			exception.printStackTrace();
			calculationsResult =  null;
		}
		return calculationsResult;
	}
	
	public Double getAverage(double[] values) {
		double sum = 0;
		try {
			int valuesCount = values.length;
			for(int i = 0; i<valuesCount; i++) {
				sum = sum + values[i];
			}
			return sum/valuesCount; 
		}catch(Exception exception) {
			exception.printStackTrace();
			return null;
		}
	}

}
