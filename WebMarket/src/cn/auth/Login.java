package cn.auth;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class Login {
	 
		DBConnect c = new DBConnect();
		Redis redis = new Redis();		
		ResultSet rs = null;
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		
     /*模拟登陆缓存, 若在缓存内部发现数据则直接读取缓存，若没有则进入数据库进行查询操作，
      * 如果数据库内部依旧没有数据，则信息输入有误或者用户未进行注册
	*/
     @Test
     public synchronized void redisLogin(String userid, String username, String pass) throws SQLException{

 	 	// redis.hset("user_info", "user_"+"1", "username_"+"Barry");
    	 
          String id = userid;
          String sql = "select * from \"user\" where id=\'"+ id +"'";
                  
          if(redis.hexists("user_"+id, "username_") && pass.equals(redis.hget("user_"+id, "userpass_"))){
                   username = redis.hget("user_"+id, "username_");
                   pass = redis.hget("user_"+id, "userpass_");
                   System.out.println("Redis! " + username + " login success");
          }else{
         	                      	 	
					rs = c.conn.createStatement().executeQuery(sql);
                   
                   if(rs.next() == false) {
                            System.out.println("information wrong! or you are not register");
//                            跳转至登陆界面
//                            register();
                   }else{
                       if(username.equals(rs.getString("name")) && pass.equals(rs.getString("password"))){
                    	   username = rs.getString("name");
                           pass = rs.getString("password");
                           
                    	   System.out.println("postgresql! " + username + " login success");
                    	   
                           redis.hset("user_"+id, "username_", username);
                           redis.hset("user_"+id, "userpass_", pass);
                           
                           //30分钟未操作就过期
                           redis.expire("user_"+id, 1800);      
                       }
                       else {
                    	   System.out.println("information wrong !!!");
                       }
                   }
          	}          
     }
     
     
     public synchronized void clusterLogin(String userid, String username, String pass) throws SQLException {
    	 
       // 最大连接数
       poolConfig.setMaxTotal(8);
 	   // 最大空闲数
 	   poolConfig.setMaxIdle(8);
 	   // 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：
 	   poolConfig.setMaxWaitMillis(1000);
    	 
 	   JedisCluster cluster = new JedisCluster(redis.PoolInitial(), poolConfig);
 	    
 	    
 	   String id = userid;
       String sql = "select * from \"user\" where id=\'"+ id +"'";
               
       if(cluster.hexists("user_"+id, "username_") && pass.equals(cluster.hget("user_"+id, "userpass_"))){
                username = cluster.hget("user_"+id, "username_");
                pass = cluster.hget("user_"+id, "userpass_");
                System.out.println("RedisCluster! " + username + " login success");
       }else{
      	                      	 	
					rs = c.conn.createStatement().executeQuery(sql);
                
                if(rs.next() == false) {
                         System.out.println("Cluster information wrong! or you are not register in redisCluster");
//                         跳转至登陆界面
//                         register();
                }else{
                    if(username.equals(rs.getString("name")) && pass.equals(rs.getString("password"))){
                 	   username = rs.getString("name");
                        pass = rs.getString("password");
                        
                 	   	System.out.println("postgresql! " + username + " login success");
                 	   
                        cluster.hset("user_"+id, "username_", username);
                        cluster.hset("user_"+id, "userpass_", pass);
                        
                        //30分钟未操作就过期
                        cluster.expire("user_"+id, 1800);      
                    }
                    else {
                 	   System.out.println("information in cluster wrong !!!");
                    }
                }
       	}
       
       try{
    	   cluster.close();
       }catch(IOException e) {
    	   e.printStackTrace();
       }
 	    	 
     }
}
