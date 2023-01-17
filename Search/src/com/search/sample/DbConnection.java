package com.search.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DbConnection {
	
	private static DbConnection single = null;

	private Connection conn;
	private PreparedStatement query;
	
	private DbConnection() throws SQLException {
		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/search","root","");
	}
	
	public static DbConnection getInstance() {
		
		if(single == null)
			try {
				single = new DbConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		
		return single;
	}
	
	public boolean addValue(SearchData obj) {
		
		try {
			query = conn.prepareStatement("insert into search_table (search_key,search_value) vsalues(?,?)");
			query.setString(1, obj.getKey());
			query.setString(2,obj.getValue());
			
			return query.execute();
			
		} catch (SQLException e) {
			
			return false;
		}
		
	}
	
	public SearchData search(String key) {
		
		try {
			query = conn.prepareStatement("select * from search_table where search_key=?");
			
			query.setString(1, key);
			
			ResultSet rs = query.executeQuery();
			
			SearchData sd = new SearchData();
			
			if(!rs.next())
				return null;
			
			sd.setId(rs.getInt(1));
			sd.setKey(rs.getString(2));
			sd.setValue(rs.getString(3));
			
			return sd;
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			return null;
		}
		
	}

}
