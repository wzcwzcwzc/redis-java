package main;

import java.sql.SQLException;

import cn.auth.Login;
import shopcart.Shopcart_op;

public class Main {

	/* 获取对应用户的购物车以及车内订单内容，并进行相应的增删改查操作
     * 在redis内存储部分热数据 包括 shopcart 与 order 存储方式为 散列方式
     * 通过散列可以达到快速查询的目的，从而优化了系统，提高了系统的运行效率
     * 对于冷数据则存储在postgresql里。
    */
	public static void main(String[] args) 
			throws SQLException {
			
		
		/*
		 * 单个Redis作为二级缓存与Postgresql连接测试
		 * 实现高速的增删改查
		 * 
		
			Login main = new Login();
			main.redisLogin("3", "Maxwell", "24");
			main.redisLogin("2", "Don", "23");
			
			
			Good computer = new Good("3", "3", "computer-3", 10000, 2 );
			
			Good coffee = new Good("2", "2", "coffee-2", 5, 10);
			
			
			Shopcart_op op = new Shopcart_op();
			

			op.addOrder("3", computer);				
			op.addOrder("2", coffee);
			op.addOrder("3", computer);	
			
			op.searchOrder("3", computer);
			op.searchOrder("2", coffee);
			
			op.calTotalMoney("3");
			op.delOrderById("3", computer);
			op.delOrderById("2", coffee);
		*/
		
		
		
		/* Redis-cluster集群测试
		 * 实现集群化的增删改查数据库操作
		 * 
		 * */
		
		
		
		Shopcart_op op = new Shopcart_op();
		
//		Login login = new Login();
//		Shopcart_op op = new Shopcart_op();
//		op.invokeBat();
		
//		login.clusterLogin("2", "Don", "23");
		long start = System.currentTimeMillis();
//		op.divOrderInClusterWithBatch();
		//无clearbatch() 1024 2s    16384 33s  
		//有clearbatch() 1024 16s	
//		op.divOrderInCluster();
		//无clearBatch() 1024 4s    16384 61s
		op.addOrder();//1024 1s	   16384  17s
		long end = System.currentTimeMillis();
		
		System.out.println((end - start) / 1000);
//		
//		long start = System.currentTimeMillis();
//		
//		op.searchOrderIDClusterAndDB("198531865710440448");
//		
//		long end = System.currentTimeMillis();
//		
//		System.out.println((end - start) / 1000);
		
		
		
//		Good computer = new Good("3", "3", "computer-3", 10000, 2 );
//		Good coffee = new Good("2", "2", "coffee-2", 5, 10);
		
		
		
//		op.addOrder("3", computer);		
//		op.addOrder("3", computer);
//		
//		op.searchOrderInCluster("3", computer);
//		op.searchOrderInCluster("2", coffee);
		
//		op.calTotalMoney("2");
//		op.searchOrder("3", computer);
		
		
		
		
//		op.modifyOrderByuseridInCluster("2", coffee, computer);
		
//		op.delOrderByIdInCluster("3", computer);
		
//		op.searchOrderInCluster("3", computer);
		
//		long start = System.currentTimeMillis();
//		op.divOrderInCluster();
//		long end = System.currentTimeMillis();
		
//		System.out.println((end - start) / 1000);
		//op.delOrderById("2", coffee);
		
		/*使用Redis集群尝试存储大量高并发的数据，模拟双十一场景
		 * 此处使用SnowFlake算法利用时间戳对所有订单进行ID分配，并存储到Redis—cluster集群中去，并记录下存储耗时
		 * */
//		Shopcart_op s = new Shopcart_op();
//		long start = System.currentTimeMillis();
//		s.redisClusterSaveID();
//		long end = System.currentTimeMillis();
//		
//		System.out.println("使用Redis集群存储8196条记录用时为 "+ (end-start)/1000 + " 秒");
		
		}
	}