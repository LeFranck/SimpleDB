package studentClient.simpledb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class tx_test_airplane {

	public static void main(String[] args) {

		
	}

	public void reserveSeat(Connection conn, int custId, int fligheId) throws SQLException
	{

		Statement stmt = conn.createStatement();
		String s;

		//Step1
		s = "select NumAvailable, Price from SEATS " 
			+ "where FlightId = " + fligheId;

		ResultSet rs = stmt.executeQuery(s);
		if(!rs.next())
		{
			System.out.println("No existe el vuelo");
			return;
		}

		int numAvailable = rs.getInt("NumAvailable");
		int price = rs.getInt("Price");
		rs.close();

		if(numAvailable == 0)
		{
			System.out.println("Vuelo lleno");
			return;
		}

		//Step2
		int numseats = numAvailable - 1;
		s = "update SEATS set NumAvailable = " 
			+ numseats + " where FlightId = " + fligheId;	
		stmt.executeQuery(s);

		//Step3
		s = "select BalanceDue from CUST " 
			+ "where CustID = " + custId;
		rs = stmt.executeQuery(s);
		int newBalance = rs.getInt("BalanceDue") + price;
		rs.close();

		s = "update CUST set BalanceDue = " 
			+ newBalance + " where CustID = " + custId;	
		stmt.executeQuery(s);


	}
}
