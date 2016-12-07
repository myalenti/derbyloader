package org.doesntexist.maysc.derbyLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import com.github.javafaker.Faker;
/**
 * Hello world!
 *
 */
public class loader
{
	private static String dbURL = "jdbc:derby://127.0.0.1:1527/test;create=true;";
	private static String tableName = ""; 
	private static Connection conn = null;
	private static Statement stmt = null;
	
    public static void main( String[] args )
    {
    	createConnection();
    	System.out.println("Creatign the necessary schema");
    	createSchema();
        System.out.println( "We will attempt to load some data into Derby" );
        for (int i = 1; i < 5; i++)
        {
        	System.out.println("Inserting Client id:" + i );
        	//insertClientDetails(i);
        	//insertClientOrders(i);
        }
        Scanner reader = new Scanner(System.in);
        System.out.println("Waiting before we cleanup ");
        int n = reader.nextInt();
        cleanup();		
    }
    public static void cleanup()
    {
    	createConnection();
    	System.out.println("Cleanup!");
    	ArrayList<String> al = new ArrayList<String>();
    	al.add("hr.employees");
    	al.add("hr.address");
    	al.add("hr.compensation");
    	al.add("crm.contacts");
    	al.add("crm.employees");
    	al.add("crm.addresses");
    	
    	ArrayList<String> alschemas = new ArrayList<String>();
    	alschemas.add("crm");
    	alschemas.add("hr");
    	
    	try
    	{
    		stmt = conn.createStatement();
    	}
    	catch (Exception except)
    	{
    		System.out.println("deleting table");
    	}
    	String sql ="";
    	al.forEach(table->{
        	System.out.println(sql);
        	try {
    			stmt.executeUpdate("DROP TABLE " + table);
    			conn.commit();
    		} catch (SQLException e) {
    			// TODO Auto-generated catch block
    			System.out.println("failed to execute to delete schemas");
    			e.printStackTrace();
    		}
    		
    	});
    	
    	alschemas.forEach(schema->{
        	System.out.println(sql);
        	try {
    			stmt.executeUpdate("DROP SCHEMA " + schema + " RESTRICT");
    			conn.commit();
    		} catch (SQLException e) {
    			// TODO Auto-generated catch block
    			System.out.println("failed to execute to delete schemas");
    			e.printStackTrace();
    		}
    		
    	});
    	
    }
    private static void createConnection()
    {
    	try
    	{
    		//Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
    		conn = DriverManager.getConnection(dbURL);
    		conn.setAutoCommit(false);
    	}
    	catch (Exception except)
    	{
    		System.out.println("Something went awry with connecting to Derby");
    		except.printStackTrace();
    		
    	}
    }
    private static void createSchema()
    {
    	try
    	{
    		stmt = conn.createStatement();
    	}
    	catch (Exception except)
    	{
    		System.out.println("error creating statement for schema creatioin");
    	}
    	String sql = "CREATE SCHEMA CRM";
    	System.out.println(sql);
    	try {
			stmt.executeUpdate(sql);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("failed to execute sql statement creating CRM schema");
			e.printStackTrace();
		}
    	System.out.println("Creating CRM.Contacts table");
    	sql = "CREATE TABLE CRM.CONTACTS " +
                "(ROW_ID INTEGER NOT NULL, " +
                " LAST_UPD VARCHAR(255), " + 
                " FIRST_NAME VARCHAR(255), " + 
                " LAST_NAME VARCHAR(255), " + 
                " email VARCHAR(255), PRIMARY KEY(ROW_ID))";
    	System.out.println(sql);
    	try {
			stmt.executeUpdate(sql);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("failed to execute sql statement building contact");
			e.printStackTrace();
		}
    	
    	System.out.println("Creating CRM.EMPLOYEES table");
    	sql = "CREATE TABLE CRM.EMPLOYEES " +
                "(ROW_ID INTEGER NOT NULL, " +
                " LAST_UPD VARCHAR(255), " + 
                " CONTACT_ID VARCHAR(255), " + 
                " LOGIN VARCHAR(255), " + 
                " DEPARTMENT VARCHAR(255), PRIMARY KEY(ROW_ID))";
    	System.out.println(sql);
    	try {
			stmt.executeUpdate(sql);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("failed to execute sql statement bulding employees");
			e.printStackTrace();
		}
    	
    	System.out.println("Creating CRM.ADDRESSES table");
    	sql = "CREATE TABLE CRM.ADDRESSES " +
                "(ROW_ID INTEGER NOT NULL, " +
                " ADDR_LINE1 VARCHAR(255), " + 
                " ADDR_LINE2 VARCHAR(255), " + 
                " CITY VARCHAR(255), " +
                " COUNTY VARCHAR(255), " +
                " COUNTRY VARCHAR(255), " + 
                " ZIPCODE VARCHAR(255), PRIMARY KEY(ROW_ID))";
    	System.out.println(sql);
    	try {
			stmt.executeUpdate(sql);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("failed to execute sql statement bulding employees");
			e.printStackTrace();
		}
    	
    	sql = "CREATE SCHEMA HR";
    	System.out.println(sql);
    	try {
			stmt.executeUpdate(sql);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("failed to execute sql statement creating HR schema");
			e.printStackTrace();
		}
    	System.out.println("Creating HR.EMPLOYEES table");
    	sql = "CREATE TABLE HR.EMPLOYEES " +
                "(ROW_ID INTEGER NOT NULL, " +
                " LAST_UPD VARCHAR(255), " + 
                " FIRST_NAME VARCHAR(255), " + 
                " LAST_NAME VARCHAR(255), " + 
                " email VARCHAR(255)," + 
                " TELEPHONE VARCHAR(255), " +
                " DEPT VARCHAR(255), " +
                " REPORTS_TO VARCHAR(255), " +
                " PRIMARY KEY(ROW_ID))";
    	System.out.println(sql);
    	try {
			stmt.executeUpdate(sql);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("failed to execute sql statement building hr.employees");
			e.printStackTrace();
		}
    	
    	System.out.println("Creating HR.ADDRESS table");
    	sql = "CREATE TABLE HR.ADDRESS " +
                "(ROW_ID INTEGER NOT NULL, " +
                " ADDR1 VARCHAR(255), " + 
                " ADDR2 VARCHAR(255), " + 
                " CITY VARCHAR(255), " + 
                " COUNTY VARCHAR(255)," +
                " COUNTRY VARCHAR(255)," +
                " ZIPCODE VARCHAR(255)," +
                " PRIMARY KEY(ROW_ID))";
    	System.out.println(sql);
    	try {
			stmt.executeUpdate(sql);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("failed to execute sql statement bulding hr.address");
			e.printStackTrace();
		}
    	
    	System.out.println("Creating CRM.COMPENSATION table");
    	sql = "CREATE TABLE HR.COMPENSATION " +
                "(ROW_ID INTEGER NOT NULL, " +
                " LAST_UPD VARCHAR(255), " + 
                " EMP_ID VARCHAR(255), " + 
                " AMOUNT VARCHAR(255), " +
                " CURRENCY VARCHAR(255), " +
                " PAY_PERIOD VARCHAR(255), " + 
                " PRIMARY KEY(ROW_ID))";
    	System.out.println(sql);
    	try {
			stmt.executeUpdate(sql);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("failed to execute sql statement bulding hr.compensation");
			e.printStackTrace();
		}
    }
    private static void insertClientDetails(int id)
    {
    	Faker faker = new Faker();
    	String fname = faker.name().firstName();
    	String lname = faker.name().lastName();
    	String address = faker.address().streetAddress();
    	String city = faker.address().cityName();
    	String state = faker.address().stateAbbr();
    	//String stmtString = "insert into " + tableName + " values(" + id + ",'"+ fname +"','" + lname + "')";
    	//System.out.println(stmtString);
    	String stmtString = "insert into " + tableName + " values(?,?,?,?,?,?)";
    	try 
    	{
    	PreparedStatement pstmt = conn.prepareStatement(stmtString);
    	pstmt.setInt(1, id);
    	pstmt.setString(2, fname);
    	pstmt.setString(3, lname);
    	pstmt.setString(4, address);
    	pstmt.setString(5, city);
    	pstmt.setString(6, state);
    	pstmt.executeUpdate();
    	}
    	catch (SQLException e)
    	{
    		e.printStackTrace();
    	}
    }
    
    private static void insertClientOrders(int id)
    {
    	Faker faker = new Faker();
    	Random rand = new Random();
    	
    	int prodCount = rand.nextInt(10);
    	for(int i=0; i< prodCount; i++)
    	{
    		int orderID = id + rand.nextInt(1000) + i;
    		int max = 100;
    		int min = 10;
    		double diff = max - min;
    		double result = min + Math.random() * diff;
    		BigDecimal dbResult = new BigDecimal(Double.toString(result));
    		//BigDecimal dbResult = new BigDecimal(result);
           // BigDecimal actrand = dblrand.divide(max, BigDecimal.ROUND_DOWN);
    		//Number total = rand.nextInt(100) + rand.nextFloat();
    		//BigDecimal total = BigDecimal( rand.nextInt(100) + rand.nextDouble() );
    		int customerID = id;
    		Timestamp tstamp = new Timestamp(new Date().getTime());
    		String stmt = "insert into orders(ORDERID, ORDER_DATE, TOTAL,CUSTOMER_ID) values (?,?,?,?)";
    		System.out.println(stmt + " " +  result);
    		try
    		{
    			PreparedStatement pstmt = conn.prepareStatement(stmt);
    			pstmt.setInt(1, orderID);
    			pstmt.setTimestamp(2, tstamp);
    			pstmt.setBigDecimal(3, dbResult.setScale(2, RoundingMode.CEILING) );
    			pstmt.setInt(4, customerID);
    			pstmt.executeUpdate();
    			
    		}
    		catch (SQLException e)
    		{
    			e.printStackTrace();
    		}
    	}
    	
    }
}
