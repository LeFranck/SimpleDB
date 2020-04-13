package studentClient.simpledb;
import java.sql.*;
import simpledb.remote.SimpleDriver;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.remote.SimpleDriver;

public class Blogs {

	public static void main(String[] args) {

		
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			stmt.executeUpdate(s);
			System.out.println("Table STUDENT created.");

			s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			String[] studvals = {"(1, 'joe', 10, 2004)",
								 "(2, 'amy', 20, 2004)",
								 "(3, 'max', 10, 2005)",
								 "(4, 'sue', 20, 2005)",
								 "(5, 'bob', 30, 2003)",
								 "(6, 'kim', 20, 2001)",
								 "(7, 'art', 30, 2004)",
								 "(8, 'pat', 20, 2001)",
								 "(9, 'lee', 10, 2004)"};
			for (int i=0; i<studvals.length; i++)
				stmt.executeUpdate(s + studvals[i]);
			System.out.println("STUDENT records inserted.");

			s = "create table BLOG(BId int, BName varchar(16), Sid int)";
			stmt.executeUpdate(s);
			System.out.println("Table BLOG created.");

			s = "insert into BLOG(BId, BName, Sid) values ";
			String[] blogtvals = {"(1, 'compsci',9)",
					 "(2, 'math',1)",
					 "(3, 'math2',1)",
					 "(4, 'math3',3)",
					 "(5, 'drama',6)"};
			for (int i=0; i<blogtvals.length; i++)
				stmt.executeUpdate(s + blogtvals[i]);
			System.out.println("BLOG records inserted.");

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
