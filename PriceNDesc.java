package StockMarket;

public class PriceNDesc{
	float price;
	String desc;
	boolean other; 
	boolean other2;
	double scale;
	
	public static PriceNDesc producePriceNDesc(float price, boolean rising, boolean falling, double scale) {
		return new PriceNDesc(price, rising, falling, scale );
		// TODO Auto-generated constructor stub
	}
	
	public PriceNDesc(float price, boolean rising, boolean falling, double scale ) {
		// TODO Auto-generated constructor stub
		this.price=price;
		this.other=rising;
		this.other2=falling;
		this.scale=scale;
	}
	
	public float getPrice() {
		return price;
	}
	public void setPrice(float price) {
		this.price = price;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public boolean isOther() {
		return other;
	}
	public void setOther(boolean other) {
		this.other = other;
	}
	public boolean isOther2() {
		return other2;
	}
	public void setOther2(boolean other2) {
		this.other2 = other2;
	} 
	public double getScale() {
		return scale;
	}
	public void setScale(double scale) {
		this.scale = scale;
	}
}