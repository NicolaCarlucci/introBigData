package generics;

import java.util.List;

/**
 * Pojo utility for extraxct date and value to line
 * 
 * @author Nicola Carlucci
 *
 */

public class LineMap {
	
	private String dataString;
	
	private List<String> value;
	
	private int intValue;
	
	private String product;
	
	
	public LineMap() {
		super();
	}

	public int getIntValue() {
		return intValue;
	}

	public void setIntValue(int intValue) {
		this.intValue = intValue;
	}

	public String getDataString() {
		return dataString;
	}

	public void setDataString(String dataString) {
		this.dataString = dataString;
	}

	public List<String> getValue() {
		return value;
	}

	public void setValue(List<String> value) {
		this.value = value;
	}

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}
	
	

}
