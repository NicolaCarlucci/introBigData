package generics;

public class Association {

	private int total = 0;
	
	private Double numSupportAssociation = 0.0;
	
	private Double numConfidentAssociation = 0.0;

	
	public Association() {
	}


	public Association(int total, Double numSupportAssociation, Double numConfidentAssociation) {
		super();
		this.total = total;
		this.numSupportAssociation = numSupportAssociation;
		this.numConfidentAssociation = numConfidentAssociation;
	}


	public int getTotal() {
		return total;
	}


	public void setTotal(int total) {
		this.total = total;
	}


	public Double getNumSupportAssociation() {
		return numSupportAssociation;
	}


	public void setNumSupportAssociation(Double numSupportAssociation) {
		this.numSupportAssociation = numSupportAssociation;
	}


	public Double getNumConfidentAssociation() {
		return numConfidentAssociation;
	}


	public void setNumConfidentAssociation(Double numConfidentAssociation) {
		this.numConfidentAssociation = numConfidentAssociation;
	}


	@Override
	public String toString() {
		return Separator.SUPPORT_ASSOCIATION + getNumSupportAssociation() + Separator.SEMICOLON + Separator.CONFIDENT_ASSOCIATION + getNumConfidentAssociation() + Separator.SEMICOLON + "i" + Separator.DOUBLE_POINTS + total;
	}
	
	
	
}
