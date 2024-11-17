package StockMarket;
import java.time.LocalDate;
public class ActualPriceDateSet {
	private LocalDate actrualPriceDate;
	private float price;
	private double scale;

	ActualPriceDateSet(LocalDate d, float price){
		actrualPriceDate=d;
		this.price=price;
	}
	ActualPriceDateSet(){
	}
	
	public double getActualScale() {
		return scale;
	}
	public void setActualScale(double scale) {
		this.scale = scale;
	}
	public float getActualPrice() {
		return price;
	}
	public void setActualPrice(float price) {
		this.price = price;
	}
	public LocalDate getActualPriceDate() {
		return actrualPriceDate;
	}
	public void setActualPriceDate(LocalDate actrualPriceDate) {
		this.actrualPriceDate = actrualPriceDate;
	}
	
	public float getActualPriceParticularDay(LocalDate d) {
		if (d.isEqual(actrualPriceDate))
			return price;
		else {
			return -1;
		}
	}
}
