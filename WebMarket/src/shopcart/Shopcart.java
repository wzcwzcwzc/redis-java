package shopcart;

public class Shopcart {

	private String user_id;
	private String order_id;
	
	Shopcart(String user_id, String order_id){
		this.user_id = user_id;
		this.order_id = order_id;
	}
	
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	
	public String getOrder_id() {
		return order_id;
	}
	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}
	
	public String toString() {
		return "userid: " + user_id + " " + "orderid: " + order_id;
	} 
	
}
