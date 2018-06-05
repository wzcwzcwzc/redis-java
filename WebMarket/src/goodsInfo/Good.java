package goodsInfo;

public class Good {

	private String userid;
	private String gid;
	private String gname;
	private int gprice;
	private int gnum;
	
	
	public Good(String userid, String gid, String gname, int gprice, int gnum){
		this.userid = userid;
		this.gid = gid;
		this.gname = gname;
		this.gnum = gnum;
		this.gprice = gprice;
	}
	
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getGid() {
		return gid;
	}
	public void setGid(String gid) {
		this.gid = gid;
	}
	public String getGname() {
		return gname;
	}
	public void setGname(String gname) {
		this.gname = gname;
	}
	public int getGnum() {
		return gnum;
	}
	public void setGnum(int gnum) {
		this.gnum = gnum;
	}
	public int getGprice() {
		return gprice;
	}
	public void setGprice(int gprice) {
		this.gprice = gprice;
	}	
}
