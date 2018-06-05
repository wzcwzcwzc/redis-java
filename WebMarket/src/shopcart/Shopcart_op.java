package shopcart;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.auth.DBConnect;
import cn.auth.Redis;
import goodsInfo.Good;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class Shopcart_op {
	
	DBConnect c = new DBConnect();
	Redis redis = new Redis();
	ResultSet rs = null;
	JedisPoolConfig poolConfig = new JedisPoolConfig();
	
	/* 根据用户id查询用户购物车内的数据，并将这些数据写入到redis缓存中去。
	 * 对数据进行基本的增删改查操作。
	 * 四种方法对应四种缓存操作
	 * 
	 * 通过输入userid与商品对象获取用户是否购买过这个商品
	 * 1.进入redis查 若存在gid则返回用户购买该商品的数目与payment
	 * 通过查询order应该返回该用户所有的订单信息
	 * */
	
	public synchronized void searchOrder(String userid, Good good) 
			throws SQLException {
		
		
		//先进入redis缓存进行数据查找，是否存在该用户的订单，若不存在则进入数据库进一步查找
		//并将该数据作为“热”数据写入到redis缓存中去，从而达到数据快速读写的功能。
		//此处的redis数据结构采用散列hash来存储，这样查询数据的时间复杂度只有O(1)
		if(redis.hgetall("good_"+good.getGid()+"|"+userid) != null) {
			//获取redis的查询结果并将其输出
			Map<String, String> good_list = new HashMap<String, String>();
			good_list = redis.hgetall("good_"+good.getGid()+"|"+userid);
			
			Set<String> set = good_list.keySet();
			Iterator<String> iterator = set.iterator();
			while(iterator.hasNext()) {
				String key = iterator.next();
				System.out.print(key + ": " + good_list.get(key)+"\t");
				System.out.println("");
			}		
		}else {
			
			String sql ="SELECT gid, gname, gprice, gnum from "
					+ "\"good\" where \"userid\" = \'"+ userid +"\'";
			
			//rs = c.conn.createStatement().executeQuery(sql);
			rs = c.conn.prepareStatement(sql).executeQuery();
			
			if(rs.next() != false) {
				
				String [] gid = new String[10];
				String [] gname = new String[10];
				int [] gprice = new int[10];
				int [] gnum = new int[10];
			
				int i = 0;
			
				if(rs.next() && i <= gid.length) {
				
					gid[i] = rs.getString("gid");
					gname[i] = rs.getString("gname");
					gprice[i] = rs.getInt("gprice");
					gnum[i] = rs.getInt("gnum");
					
					System.out.println("gid_"+gid[i] + "\t gname_"+gname[i] 
							+ "\t gprice_"+gprice[i] + "\t gnum_"+gnum[i]);
					System.out.println("");
				}
			}
			
			HashMap<String, String> good_list = new HashMap<String, String>();
			
			good_list.put("userid", userid);
			good_list.put("gid", good.getGid());
			good_list.put("gname", good.getGname());
			good_list.put("gprice", String.valueOf(good.getGprice()));
			good_list.put("gnum", String.valueOf(good.getGnum()));
		
			redis.hmset("good_"+good.getGid()+"|"+userid, good_list);
			redis.setex("\"good_\"+good.getGid()+\"|\"+userid", 6, null);
			redis.expire("good_"+good.getGid()+"|"+userid, 1800);
		}
		
	}
	
	public synchronized void searchOrderInCluster(String userid, Good good) 
			throws SQLException {
		
	    poolConfig.setMaxTotal(8);	  
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
		
	    JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		
		if(cluster.hgetAll("good_"+good.getGid()+"|"+userid) != null) {
			//获取redis的查询结果并将其输出
			Map<String, String> good_list = new HashMap<String, String>();
			good_list = cluster.hgetAll("good_"+good.getGid()+"|"+userid);
			
			Set<String> set = good_list.keySet();
			Iterator<String> iterator = set.iterator();
			while(iterator.hasNext()) {
				String key = iterator.next();
				System.out.print(key + ": " + good_list.get(key)+"\t");
				System.out.println("");
			}		
		}else {
			
			String sql ="SELECT gid, gname, gprice, gnum from "
					+ "\"good\" where \"userid\" = \'"+ userid +"\'";
			
			//rs = c.conn.createStatement().executeQuery(sql);

			rs = c.conn.prepareStatement(sql).executeQuery();
			if(rs.next() != false) {
				
				String [] gid = new String[10];
				String [] gname = new String[10];
				int [] gprice = new int[10];
				int [] gnum = new int[10];
			
				int i = 0;
			
				if(rs.next() && i <= gid.length) {
				
					gid[i] = rs.getString("gid");
					gname[i] = rs.getString("gname");
					gprice[i] = rs.getInt("gprice");
					gnum[i] = rs.getInt("gnum");
					
					System.out.println("gid_"+gid[i] + "\t gname_"
					+ gname[i] + "\t gprice_"+gprice[i] + "\t gnum_"+gnum[i]);
					System.out.println("");
				}
			}
			
			HashMap<String, String> good_list = new HashMap<String, String>();
			
			good_list.put("userid", userid);
			good_list.put("gid", good.getGid());
			good_list.put("gname", good.getGname());
			good_list.put("gprice", String.valueOf(good.getGprice()));
			good_list.put("gnum", String.valueOf(good.getGnum()));
			
			Set<String> set = good_list.keySet();
			Iterator<String> iterator = set.iterator();
			while(iterator.hasNext()) {
				String key = iterator.next();
				System.out.print(key + ": " + good_list.get(key)+"\t");
				System.out.println("");
			}		
		
			cluster.hmset("good_"+good.getGid()+"|"+userid, good_list);
			cluster.setex("good_"+good.getGid()+"|"+userid, 60, null);
			cluster.expire("good_"+good.getGid()+"|"+userid, 1800);
		}
		
		try {
			cluster.close();
		}catch(IOException e) {
			e.getMessage();
		}
	
	}
	
	
	/*search for order id by redis and posgresql*/
	public synchronized void searchOrderIDClusterAndDB(String orderid) {
		

		poolConfig.setMaxTotal(8);	  
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
		
		JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		
		List<String> id_list = new LinkedList<String>();
		
//		for(long i = 0; i < cluster.llen("goodID_list"); i++ ) {
//			
//			id_list.add(cluster.lindex("goodID_list", i));
//			
//		}
		
		for(long i = 0; i < cluster.llen("orderid_list"); i++ ) {
			
			id_list.add(cluster.lindex("orderid_list", i));
			
		}
		
//		System.out.println(id_list.get(100));
//		
//		if(id_list.contains(id_list.get(100))) {
//			System.out.println("hehe " + id_list.indexOf("196772915604369408"));
//		}
		
		if(id_list.contains(orderid)) {
			
			System.out.println("the orderid can be found "
					+ "in redis-cluster and its index is: " + id_list.indexOf(orderid));
			
		}else {
			
			long orderID = Long.parseLong(orderid);
			
			int tbsum = 5;
			int dbsum = 1;
			int temp = 0;
			
			temp = (int)(orderID % (tbsum * dbsum));
			
			int dbnum = (int)Math.floor(temp / tbsum);
			int tbnum =  temp % tbsum;
			
			String sql = "select \"orderID\" from public.order_"+ tbnum + " where \"orderID\" =\'" + orderid + "\';";
			
			try {
				rs = c.conn.prepareStatement(sql).executeQuery();
				
				if(rs.next()) {
					
					System.out.println("the orderid is in db_"+ dbnum + " tbnum_"+tbnum);
					
				}else {
					System.out.println("the orderid is not exist!");
				}
				cluster.close();
			}catch(SQLException e) {
				e.getMessage();
			}catch(IOException e) {
				e.getMessage();
			}
		}
	}
	
	
	
	public synchronized void modifyOrderByuserid(String userid, Good good, Good nGood) 
			throws SQLException {
			
			redis.hdel("good_"+good.getGid()+"|"+userid, good.getGid());
			redis.hdel("good_"+good.getGid()+"|"+userid, good.getGname());
			redis.hdel("good_"+good.getGid()+"|"+userid, String.valueOf(good.getGnum()));
			redis.hdel("good_"+good.getGid()+"|"+userid, String.valueOf(good.getGprice()));
			
			HashMap<String, String> good_list = new HashMap<String, String>();
			
			good_list.put("userid", userid);
			good_list.put("gid", nGood.getGid());
			good_list.put("gname", nGood.getGname());
			good_list.put("gprice", String.valueOf(nGood.getGprice()));
			good_list.put("gnum", String.valueOf(nGood.getGnum()));
			
			redis.hmset("good_"+good.getGid()+"|"+userid, good_list);
			redis.expire("good_"+good.getGid()+"|"+userid, 1800);
			
			//redis内不存在该数据，则进入数据库进行查询	
			String sql = "UPDATE public.good\r\n" + 
					"	SET userid=\'"+userid+"\', gid=\'"+nGood.getGid()+
					"\', gname=\'"+nGood.getGname()+"\', gprice=\'"+nGood.getGprice()+
					"\', gnum=\'"+nGood.getGnum()+"\'\r\n" + 
					"	WHERE userid=\'"+userid+"\'";		
			
			try {
				//c.conn.createStatement().executeQuery(sql);
				c.conn.prepareStatement(sql).executeQuery();
			}catch(SQLException e) {
				e.getMessage();
			}
				
	}
	
	
	public synchronized void modifyOrderByuseridInCluster(String userid, Good good, Good nGood) 
			throws SQLException {
	
		poolConfig.setMaxTotal(8);	  
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
		
	    JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		
		cluster.hdel("good_"+good.getGid()+"|"+userid, good.getGid());
		cluster.hdel("good_"+good.getGid()+"|"+userid, good.getGname());
		cluster.hdel("good_"+good.getGid()+"|"+userid, String.valueOf(good.getGnum()));
		cluster.hdel("good_"+good.getGid()+"|"+userid, String.valueOf(good.getGprice()));
		
		HashMap<String, String> good_list = new HashMap<String, String>();
		
		good_list.put("userid", userid);
		good_list.put("gid", nGood.getGid());
		good_list.put("gname", nGood.getGname());
		good_list.put("gprice", String.valueOf(nGood.getGprice()));
		good_list.put("gnum", String.valueOf(nGood.getGnum()));
	
		cluster.hmset("good_"+nGood.getGid()+"|"+userid, good_list);
		cluster.expire("good_"+nGood.getGid()+"|"+userid, 1800);
		
		//redis内不存在该数据，则进入数据库进行查询	
		String sql = "UPDATE public.good\r\n" + 
				"	SET userid=\'"+userid+"\', gid=\'"+nGood.getGid()+
				"\', gname=\'"+nGood.getGname()+"\', gprice=\'"+nGood.getGprice()+
				"\', gnum=\'"+nGood.getGnum()+"\'\r\n" + 
				"	WHERE userid=\'"+userid+"\'";
		
		try {

			c.conn.prepareStatement(sql).executeQuery();
//			c.conn.createStatement().executeQuery(sql);
			cluster.close();
			
		}catch(SQLException e) {
			e.getMessage();
		}catch(IOException e) {
			e.getMessage();
		}
			
}
	
	
	//采用hash散列来存储用户的购物车内容，将这些内容暂时存储到redis中，经过一段时间自动更新
	public synchronized void addOrder(String userid, Good good) 
			throws SQLException {
		
			//将商品信息增加到redis的对应userid的good_list中去
			HashMap<String, String> good_list = new HashMap<String, String>();
		
			good_list.put("userid", userid);
			good_list.put("gid", good.getGid());
			good_list.put("gname", good.getGname());
			good_list.put("gprice", String.valueOf(good.getGprice()));
			good_list.put("gnum", String.valueOf(good.getGnum()));
		
			redis.hmset("good_"+good.getGid()+"|"+userid, good_list);
			redis.expire("good_"+good.getGid()+"|"+userid, 1800);
		
			//在对redis操作完毕后，再对数据库进行操作
	
			String sql = "INSERT INTO public.good(\r\n" + 
					"	userid, gid, gname, gprice, gnum)\r\n" + 
					"	VALUES (\'" + userid + "\'"
					+ ",\'" + good.getGid() + "\'"
					+",\'" + good.getGname() + "\'"
					+"," + good.getGprice() 
					+"," + good.getGnum() + ")";
		
			
			//若不加try-catch 模块就会出现中断,怀疑是使用connection时出现异常
			try {
//				c.conn.createStatement().executeQuery(sql);
				c.conn.prepareStatement(sql).executeQuery();
			}catch(SQLException e) {
				e.getMessage();
			}
	}
	
	
	public synchronized void addOrderInCluster(String userid, Good good) 
			throws SQLException {
		
		poolConfig.setMaxTotal(8);	  
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
		
	    JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		
		//将商品信息增加到redis的对应userid的good_list中去
		HashMap<String, String> good_list = new HashMap<String, String>();
	
		good_list.put("userid", userid);
		good_list.put("gid", good.getGid());
		good_list.put("gname", good.getGname());
		good_list.put("gprice", String.valueOf(good.getGprice()));
		good_list.put("gnum", String.valueOf(good.getGnum()));
	
		cluster.hmset("good_"+good.getGid()+"|"+userid, good_list);
		cluster.expire("good_"+good.getGid()+"|"+userid, 1800);
	
		//在对redis操作完毕后，再对数据库进行操作

		String sql = "INSERT INTO public.good(\r\n" + 
				"	userid, gid, gname, gprice, gnum)\r\n" + 
				"	VALUES (\'" + userid + "\'"
				+ ",\'" + good.getGid() + "\'"
				+",\'" + good.getGname() + "\'"
				+"," + good.getGprice() 
				+"," + good.getGnum() + ")";
	
		
		//若不加try-catch 模块就会出现中断,怀疑是使用connection时出现异常
		try {

			c.conn.prepareStatement(sql).executeQuery();
//			c.conn.createStatement().executeQuery(sql);
			cluster.close();
		}catch(SQLException e) {
			e.getMessage();
		}catch(IOException e) {
			e.getMessage();
		}
}
	
	
	public synchronized void delOrderById(String userid, Good good) 
			throws SQLException{
		
			redis.del("good_"+good.getGid()+"|"+userid);
			
			String sql = "delete from \"good\" where userid=\'"+ userid +"\' and "
					+ "gid=\'"+good.getGid()+"\'";
			
			try {
				c.conn.prepareStatement(sql).executeQuery();
			}catch(SQLException e) {
				e.getMessage();
			}
	}
	
	public synchronized void delOrderByIdInCluster(String userid, Good good) 
			throws SQLException{
		
		
		poolConfig.setMaxTotal(8);	  
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
		
	    JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		
		cluster.del("good_"+good.getGid()+"|"+userid);
		
		String sql = "delete from \"good\" where userid=\'"+ userid +"\' and "
				+ "gid=\'"+good.getGid()+"\'";
		
		try {
//			c.conn.createStatement().executeQuery(sql);
			c.conn.prepareStatement(sql).executeQuery();
			cluster.close();
		}catch(SQLException e) {
			e.getMessage();
		}catch(IOException e) {
			e.getMessage();
		}
}
	
	
	public synchronized void showAllOrdersByuserid(String userid) 
			throws SQLException {
		
		/* usergood_userid -- gname1
		 * 			  		  gname2
		 * 			  		  gname3
		 * 			  	 	  gname4
		 * 
		 * gid_1 -- gid_
		 * 			gname_
		 * 			gprice_
		 * 			gnum_
		 * 
		 * user_id -- userpass_
		 * 			  username_
		 * 
		 * */		
		
		if(redis.smembers("usergood_" + userid) != null && 
				redis.exists("usergood_" + userid)){
		
			
			Set<String> good_set = new HashSet<String>();
			
			good_set.addAll(redis.smembers("usergood_"+userid));
			
			Iterator<String> iterator = good_set.iterator();
			
			while(iterator.hasNext()) {
				
				System.out.print("userid: "+userid+" goods_name: " 
				+ iterator.next() + "\r");
			}
			
		}else{
			
			String sql = "SELECT gid, gname, gprice, gnum\r\n" + 
					"	FROM \"good\" where userid =\'" + userid + "\'";
			
			//rs = c.conn.createStatement().executeQuery(sql);

			rs = c.conn.prepareStatement(sql).executeQuery();
			
			int i = 0;
			
			String[] gid = new String[10];
			String[] gname = new String[10];
			int[] gprice = new int[10];
			int[] gnum = new int[10];
			
			while(i < gid.length && rs.next()) {
				
				gid[i] = rs.getString("gid");
				gname[i] = rs.getString("gname");
				gprice[i] = rs.getInt("gprice");
				gnum[i] = rs.getInt("gnum");
				
				
				System.out.println("gid: " + gid[i] + "\tgname: " + gname[i] 
						+ "\tgprice: " + gprice[i] + "\tgnum: " + gnum[i]);
				//将该查询数据作为热数据存储到redis中并进行更新	
				redis.sadd("usergood_"+userid, ""+gname[i]);//对集合进行操作
				redis.expire("usergood_"+userid, 1800);
				
				
			}
			
		}
	}
	
	
	public synchronized void showAllOrdersByuseridInCluster(String userid) 
			throws SQLException{
		
		poolConfig.setMaxTotal(8);	  
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
		
		JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		
		if(cluster.smembers("usergood_" + userid) != null 
				&& cluster.exists("usergood_" + userid)){
		
			
			Set<String> good_set = new HashSet<String>();
			
			good_set.addAll(cluster.smembers("usergood_"+userid));
			
			Iterator<String> iterator = good_set.iterator();
			
			while(iterator.hasNext()) {
				
				System.out.print("userid: "+userid+" goods_name: " 
				+ iterator.next() + "\r");
			}
			
		}else{
			
			String sql = "SELECT gid, gname, gprice, gnum\r\n" + 
					"	FROM \"good\" where userid =\'" + userid + "\'";

			rs = c.conn.prepareStatement(sql).executeQuery();
//			rs = c.conn.createStatement().executeQuery(sql);
			
			int i = 0;
			
			String[] gid = new String[10];
			String[] gname = new String[10];
			int[] gprice = new int[10];
			int[] gnum = new int[10];
			
			while(i < gid.length && rs.next()) {
				
				gid[i] = rs.getString("gid");
				gname[i] = rs.getString("gname");
				gprice[i] = rs.getInt("gprice");
				gnum[i] = rs.getInt("gnum");
				
				
				System.out.println("gid: " + gid[i] + "\tgname: " + gname[i] 
						+ "\tgprice: " + gprice[i] + "\tgnum: " + gnum[i]);
				//将该查询数据作为热数据存储到redis中并进行更新	
				cluster.sadd("usergood_"+userid, ""+gname[i]);//对集合进行操作
				cluster.expire("usergood_"+userid, 1800);
			}
		}

		try {
			cluster.close();
		}catch(IOException e) {
			e.getMessage();
		}
	}
	
	public void calTotalMoney(String userid) 
			throws SQLException {
		
		
		String sql = "SELECT sum(gnum * gprice)\r\n" + 
				"	FROM public.good where userid =\'"+userid+"\'";
		
		try {

			rs = c.conn.prepareStatement(sql).executeQuery();
//			rs = c.conn.createStatement().executeQuery(sql);
			
			int total = 0;
			
			if(rs.next()) {
				
				total = rs.getInt("sum");
				System.out.println("the total money of user "+ userid + ": " + total);
			}
			
		} catch (Exception e) {
			e.getMessage();
		}		
	}
	
		
		 /*进行redis的操作
		    *1. 调用snowFlake生成大量ID用来模拟大量数据访问
		    *2. 将这些ID进行分库分表操作，先存入Redis集群
		    *3. Redis集群作为二级缓存与后台数据库相连接，不断向数据库内存储信息
		    * 对集群内部的订单编号进行相应的分库分表操作
		 * */
	
	/*
	 * need improvement
	 * 
	 * */
	public void divOrderInClusterWithBatch() 
			throws SQLException {
		
	    poolConfig.setMaxTotal(8);
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
	    
		JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		SnowFlake snow = new SnowFlake(2, 3);
		
		
		
		//拆分dbsum个库  每个库tbsum张表
		
		int dbnum, tbnum = 0;
		final int tbsum = 5;
		final int dbsum = 1;
		int temp = 0;
		
		for(int i = 0; i < (1 << 10); i++) {
			
			long orderid = snow.nextId();
			
			temp = (int)(orderid % (tbsum * dbsum));
			
			dbnum = (int)Math.floor(temp / tbsum);
			
			tbnum =  temp % tbsum;
			
//			cluster.lpush("id_list", "dbnum_" + dbnum + "|tbnum_" + tbnum + "|" + orderid);
			
			cluster.lpush("orderid_list", String.valueOf(orderid));
			
//			addSnowIDInDB(dbnum, tbnum, String.valueOf(orderid));	
					
				
		}
		
		List<String> id_list = new LinkedList<String>();
		
		for(int i = 0; i < (1 << 10); i++) {

			id_list.add(cluster.lindex("orderid_list", i));
			
		}
		
		String[] sqls = new String[id_list.size()];
		
		for(int i = 0; i < id_list.size(); i++) {
			
			temp = (int)(Long.parseLong(id_list.get(i)) % (tbsum * dbsum));	
			dbnum = (int)Math.floor(temp / tbsum);
			tbnum =  temp % tbsum;
				
			sqls[i] = "INSERT INTO public.order_" + tbnum + "(\r\n" + 
					"	\"orderID\")\r\n" + 
					"	VALUES (\'" + id_list.get(i) + "\');";	

		}	
			//批处理操作
			Statement stmt = c.conn.createStatement();
			
			for(int i = 0; i < id_list.size(); i++) {
				
				stmt.addBatch(sqls[i]);
				
				while(i % 600 == 0) {
					
					stmt.executeBatch();
					i++;
//					stmt.clearBatch();
				
				}
			}
			
//			Statement stmt = c.conn.createStatement();	
////			PreparedStatement ps = null;
////			
//			try {			
//				for(int i = 0; i < id_list.size(); i++) {	
//					
//					stmt.addBatch(sqls[i]);
////					stmt.setString();
//					System.out.println("batch:::::"+sqls[i]);
//					while(i % 30 == 0) {
//						stmt.executeBatch();
//						System.out.println("execute------------"+sqls[i]);
//					}
//				}
//			}catch(SQLException e) {
//				e.getMessage();
//			}
			
			try {
				cluster.close();
				System.out.println("the id has been divided into different db and tb");
			}catch(IOException e) {
				e.getMessage();
			}
		}
	
	public void divOrderInCluster() throws SQLException {
		poolConfig.setMaxTotal(8);
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
	    
		JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		SnowFlake snow = new SnowFlake(2, 3);

		
		
		//拆分dbsum个库  每个库tbsum张表
		
		int dbnum, tbnum = 0;
		int tbsum = 5;
		int dbsum = 1;
		int temp = 0;
		
		for(int i = 0; i < (1 << 10); i++) {
			
			long orderid = snow.nextId();
			
			temp = (int)(orderid % (tbsum * dbsum));
			
			dbnum = (int)Math.floor(temp / tbsum);
			
			tbnum =  temp % tbsum;
			
//			cluster.lpush("id_list", "dbnum_" + dbnum + "|tbnum_" + tbnum + "|" + orderid);
			
			cluster.lpush("orderid_list", String.valueOf(orderid));
			
			addSnowIDWithoutBatch(dbnum, tbnum, String.valueOf(orderid));	
		
		}
		try {
			cluster.close();
		}catch(IOException e) {
			e.getMessage();
			
		}
	}
	
	
	public void addSnowIDWithoutBatch(int dbnum, int tbnum, String orderid) {
		
		String sql = "INSERT INTO public.order_" + tbnum + "(\r\n" + 
						"	\"orderID\")\r\n" + 
						"	VALUES (\'" + orderid + "\');";
		
		try {
			c.conn.createStatement().executeQuery(sql);
			
		}catch(SQLException e) {
			e.getMessage();
		}
		
	}
		
	
	
	
	/*
	 * 尝试加入批处理操作 使用preparedStatement处理相应的sql语句 加快插入数据的速度 need improvement
	 * 
	 * 
	 * ________     _________ | | | 
	 						  | | | 
	 * 						  |
	 * 						  |
	 * 
	 * 
	 * 		________
	 * 
	 * 
	 * 
	 * 
	 * */
	public synchronized void addSnowIDInDBWithBatch(int dbnum, int tbnum, String orderid) 
			throws SQLException{
		
		poolConfig.setMaxTotal(8);	  
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
		
		JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		
		List<String> id_list = new LinkedList<String>();
		
		for(long i = 0; i < cluster.llen("orderid_list"); i++ ) {
			
			id_list.add(cluster.lindex("orderid_list", i));
			
		}
		
		for(int i = 0; i < id_list.size(); i++) {
			
			String sql = "INSERT INTO public.order_" + tbnum + "(\r\n" + 
					"	\"orderID\")\r\n" + 
					"	VALUES (\'" + orderid + "\');";
		

			PreparedStatement ps = null;
			
			try {
				ps = c.conn.prepareStatement(sql);
				ps.addBatch();
				
				while(i % 1000 == 0) {
				
					ps.executeBatch();
				
				}
			}catch(SQLException e) {
				e.getMessage();
			}
			
		}
		
		try {		
			cluster.close();		
		}catch(IOException e) {
			e.getMessage();
		}
	}
	
	
	public synchronized void addOrder() {
		
		poolConfig.setMaxTotal(8);
	    poolConfig.setMaxIdle(8);
	    poolConfig.setMaxWaitMillis(1000);
	    
		JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
		SnowFlake snow = new SnowFlake(2, 3);
		
		for(int i = 0; i < (1 << 10); i++) {
			
			
			cluster.lpush("orderid_list", String.valueOf(snow.nextId()));
		
		}
		try {
			cluster.close();
		}catch(IOException e) {
			e.getMessage();
			
		}
		
	}
	
	/*
	 * 调用bat文件 but has some problems about cluster down
	 * */
//	public void invokeBat() {
//		
//		String cmd = "cmd /k start E:\\RedisCluster\\startCluster.bat";
//		
//		try {
//			Process ps = Runtime.getRuntime().exec(cmd);
//			System.out.println(ps.getInputStream());
//			
//		}catch(IOException e) {
//			
//			e.printStackTrace();
//		}
//		
//	}
}

