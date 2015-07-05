package mimir;

import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import net.sf.jsqlparser.expression.Expression;

import oracle.jdbc.pool.OracleDataSource;

public class Methods {
	static HashMap<String,Integer> temp;
	static HashMap<String,HashMap<Integer,Integer>> map=new HashMap<String,HashMap<Integer,Integer>>();
	static TreeMap<String,Integer> sorted_map;
	static List<String> result;
	public static Connection getConn(){
		Connection conn=null;
		OracleDataSource ods;
		String url = "???";
		try {
			ods = new OracleDataSource();
        // Set the user name, password, driver type and network protocol    
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream("config/jdbc.property");
        //loading properites from properties file
        props.loadFromXML(fis);
        //reading proeprty
        ods.setUser(props.getProperty("user"));
        ods.setPassword(props.getProperty("password"));
        url=props.getProperty("url");
        ods.setURL(url);
        // Retrieve a connection                                                                                                                                                                            
         conn = ods.getConnection();
		} catch (Exception e) {
  		  System.err.println("Connection failed to : "+url);
			e.printStackTrace();
		}
		return conn;
		
	}
}
