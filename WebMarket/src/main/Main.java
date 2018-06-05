package main;

import java.sql.SQLException;

import cn.auth.Login;
import shopcart.Shopcart_op;

public class Main {

	/* ��ȡ��Ӧ�û��Ĺ��ﳵ�Լ����ڶ������ݣ���������Ӧ����ɾ�Ĳ����
     * ��redis�ڴ洢���������� ���� shopcart �� order �洢��ʽΪ ɢ�з�ʽ
     * ͨ��ɢ�п��Դﵽ���ٲ�ѯ��Ŀ�ģ��Ӷ��Ż���ϵͳ�������ϵͳ������Ч��
     * ������������洢��postgresql�
    */
	public static void main(String[] args) 
			throws SQLException {
			
		
		/*
		 * ����Redis��Ϊ����������Postgresql���Ӳ���
		 * ʵ�ָ��ٵ���ɾ�Ĳ�
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
		
		
		
		/* Redis-cluster��Ⱥ����
		 * ʵ�ּ�Ⱥ������ɾ�Ĳ����ݿ����
		 * 
		 * */
		
		
		
		Shopcart_op op = new Shopcart_op();
		
//		Login login = new Login();
//		Shopcart_op op = new Shopcart_op();
//		op.invokeBat();
		
//		login.clusterLogin("2", "Don", "23");
		long start = System.currentTimeMillis();
//		op.divOrderInClusterWithBatch();
		//��clearbatch() 1024 2s    16384 33s  
		//��clearbatch() 1024 16s	
//		op.divOrderInCluster();
		//��clearBatch() 1024 4s    16384 61s
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
		
		/*ʹ��Redis��Ⱥ���Դ洢�����߲��������ݣ�ģ��˫ʮһ����
		 * �˴�ʹ��SnowFlake�㷨����ʱ��������ж�������ID���䣬���洢��Redis��cluster��Ⱥ��ȥ������¼�´洢��ʱ
		 * */
//		Shopcart_op s = new Shopcart_op();
//		long start = System.currentTimeMillis();
//		s.redisClusterSaveID();
//		long end = System.currentTimeMillis();
//		
//		System.out.println("ʹ��Redis��Ⱥ�洢8196����¼��ʱΪ "+ (end-start)/1000 + " ��");
		
		}
	}