package studentClient.simpledb;
import java.sql.*;
import simpledb.remote.SimpleDriver;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.remote.SimpleDriver;

public class Cities {

	public static void main(String[] args) {

		
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			String s = "create table TRIP(SId int, Origen varchar(16), Destino varchar(16))";
			stmt.executeUpdate(s);
			System.out.println("Table TRIP created.");

			s = "insert into TRIP(SId, Origen, Destino) values ";
			String[] studvals = {"(1, 'Santiago', 'vina')",
								 "(2, 'vina',  'La serena')",
								 "(3, 'Santiago',  'Rancagua')",
								 "(4, 'Rancagua', 'Talca')",
								 "(5, 'Talca',  'Los Angeles')",
								 "(6, 'Los Angeles', 'conce')"};
			for (int i=0; i<studvals.length; i++)
				stmt.executeUpdate(s + studvals[i]);
			System.out.println("TRIP records inserted.");

		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		
		
	}

}