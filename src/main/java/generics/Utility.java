package generics;

public class Utility {

	private static int max = 10000;
	private static int min = 1;
	static int range = max - min + 1;
	
	/**
	 * Return random between 1 and 10000
	 * 
	 * @return
	 */
	public static String getMathRandom() {
		return String.valueOf((int)(Math.random() * range) + min);
	}
	
}
