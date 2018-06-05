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
	
	/* �����û�id��ѯ�û����ﳵ�ڵ����ݣ�������Щ����д�뵽redis������ȥ��
	 * �����ݽ��л�������ɾ�Ĳ������
	 * ���ַ�����Ӧ���ֻ������
	 * 
	 * ͨ������userid����Ʒ�����ȡ�û��Ƿ���������Ʒ
	 * 1.����redis�� ������gid�򷵻��û��������Ʒ����Ŀ��payment
	 * ͨ����ѯorderӦ�÷��ظ��û����еĶ�����Ϣ
	 * */
	
	public synchronized void searchOrder(String userid, Good good) 
			throws SQLException {
		
		
		//�Ƚ���redis����������ݲ��ң��Ƿ���ڸ��û��Ķ���������������������ݿ��һ������
		//������������Ϊ���ȡ�����д�뵽redis������ȥ���Ӷ��ﵽ���ݿ��ٶ�д�Ĺ��ܡ�
		//�˴���redis���ݽṹ����ɢ��hash���洢��������ѯ���ݵ�ʱ�临�Ӷ�ֻ��O(1)
		if(redis.hgetall("good_"+good.getGid()+"|"+userid) != null) {
			//��ȡredis�Ĳ�ѯ������������
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
			//��ȡredis�Ĳ�ѯ������������
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
			
			//redis�ڲ����ڸ����ݣ���������ݿ���в�ѯ	
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
		
		//redis�ڲ����ڸ����ݣ���������ݿ���в�ѯ	
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
	
	
	//����hashɢ�����洢�û��Ĺ��ﳵ���ݣ�����Щ������ʱ�洢��redis�У�����һ��ʱ���Զ�����
	public synchronized void addOrder(String userid, Good good) 
			throws SQLException {
		
			//����Ʒ��Ϣ���ӵ�redis�Ķ�Ӧuserid��good_list��ȥ
			HashMap<String, String> good_list = new HashMap<String, String>();
		
			good_list.put("userid", userid);
			good_list.put("gid", good.getGid());
			good_list.put("gname", good.getGname());
			good_list.put("gprice", String.valueOf(good.getGprice()));
			good_list.put("gnum", String.valueOf(good.getGnum()));
		
			redis.hmset("good_"+good.getGid()+"|"+userid, good_list);
			redis.expire("good_"+good.getGid()+"|"+userid, 1800);
		
			//�ڶ�redis������Ϻ��ٶ����ݿ���в���
	
			String sql = "INSERT INTO public.good(\r\n" + 
					"	userid, gid, gname, gprice, gnum)\r\n" + 
					"	VALUES (\'" + userid + "\'"
					+ ",\'" + good.getGid() + "\'"
					+",\'" + good.getGname() + "\'"
					+"," + good.getGprice() 
					+"," + good.getGnum() + ")";
		
			
			//������try-catch ģ��ͻ�����ж�,������ʹ��connectionʱ�����쳣
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
		
		//����Ʒ��Ϣ���ӵ�redis�Ķ�Ӧuserid��good_list��ȥ
		HashMap<String, String> good_list = new HashMap<String, String>();
	
		good_list.put("userid", userid);
		good_list.put("gid", good.getGid());
		good_list.put("gname", good.getGname());
		good_list.put("gprice", String.valueOf(good.getGprice()));
		good_list.put("gnum", String.valueOf(good.getGnum()));
	
		cluster.hmset("good_"+good.getGid()+"|"+userid, good_list);
		cluster.expire("good_"+good.getGid()+"|"+userid, 1800);
	
		//�ڶ�redis������Ϻ��ٶ����ݿ���в���

		String sql = "INSERT INTO public.good(\r\n" + 
				"	userid, gid, gname, gprice, gnum)\r\n" + 
				"	VALUES (\'" + userid + "\'"
				+ ",\'" + good.getGid() + "\'"
				+",\'" + good.getGname() + "\'"
				+"," + good.getGprice() 
				+"," + good.getGnum() + ")";
	
		
		//������try-catch ģ��ͻ�����ж�,������ʹ��connectionʱ�����쳣
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
				//���ò�ѯ������Ϊ�����ݴ洢��redis�в����и���	
				redis.sadd("usergood_"+userid, ""+gname[i]);//�Լ��Ͻ��в���
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
				//���ò�ѯ������Ϊ�����ݴ洢��redis�в����и���	
				cluster.sadd("usergood_"+userid, ""+gname[i]);//�Լ��Ͻ��в���
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
	
		
		 /*����redis�Ĳ���
		    *1. ����snowFlake���ɴ���ID����ģ��������ݷ���
		    *2. ����ЩID���зֿ�ֱ�������ȴ���Redis��Ⱥ
		    *3. Redis��Ⱥ��Ϊ�����������̨���ݿ������ӣ����������ݿ��ڴ洢��Ϣ
		    * �Լ�Ⱥ�ڲ��Ķ�����Ž�����Ӧ�ķֿ�ֱ����
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
		
		
		
		//���dbsum����  ÿ����tbsum�ű�
		
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
			//���������
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

		
		
		//���dbsum����  ÿ����tbsum�ű�
		
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
	 * ���Լ������������ ʹ��preparedStatement������Ӧ��sql��� �ӿ�������ݵ��ٶ� need improvement
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
	 * ����bat�ļ� but has some problems about cluster down
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

