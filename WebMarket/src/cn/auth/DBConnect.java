package cn.auth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnect {
        
	public Connection conn;
	
        {
                try {
                         Class.forName("org.postgresql.Driver");
                         conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/market","postgres","1997625wzc");
                } catch (ClassNotFoundException e) {
                         e.printStackTrace();
                } catch (SQLException e) {
                         e.printStackTrace();
                }
        }
	}			
		
			
	
