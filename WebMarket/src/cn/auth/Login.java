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
		
     /*ģ���½����, ���ڻ����ڲ�����������ֱ�Ӷ�ȡ���棬��û����������ݿ���в�ѯ������
      * ������ݿ��ڲ�����û�����ݣ�����Ϣ������������û�δ����ע��
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
//                            ��ת����½����
//                            register();
                   }else{
                       if(username.equals(rs.getString("name")) && pass.equals(rs.getString("password"))){
                    	   username = rs.getString("name");
                           pass = rs.getString("password");
                           
                    	   System.out.println("postgresql! " + username + " login success");
                    	   
                           redis.hset("user_"+id, "username_", username);
                           redis.hset("user_"+id, "userpass_", pass);
                           
                           //30����δ�����͹���
                           redis.expire("user_"+id, 1800);      
                       }
                       else {
                    	   System.out.println("information wrong !!!");
                       }
                   }
          	}          
     }
     
     
     public synchronized void clusterLogin(String userid, String username, String pass) throws SQLException {
    	 
       // ���������
       poolConfig.setMaxTotal(8);
 	   // ��������
 	   poolConfig.setMaxIdle(8);
 	   // �������ȴ�ʱ�䣬����������ʱ�仹δ��ȡ�����ӣ���ᱨJedisException�쳣��
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
//                         ��ת����½����
//                         register();
                }else{
                    if(username.equals(rs.getString("name")) && pass.equals(rs.getString("password"))){
                 	   username = rs.getString("name");
                        pass = rs.getString("password");
                        
                 	   	System.out.println("postgresql! " + username + " login success");
                 	   
                        cluster.hset("user_"+id, "username_", username);
                        cluster.hset("user_"+id, "userpass_", pass);
                        
                        //30����δ�����͹���
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
