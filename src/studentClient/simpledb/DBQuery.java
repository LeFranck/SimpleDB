package studentClient.simpledb;

import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class DBQuery {
	public static void main(String[] args) {
		SimpleDB.init("lab4_test1");
		
		String query_a = "select a1, a2 from a";
		String query_b = "select b1, b2 from b";
		
		Transaction tx = new Transaction();
		Scan scan_a = SimpleDB.planner().createQueryPlan(query_a, tx).open();
		System.out.println("\nA1\tA2");
		while (scan_a.next()) {
			//SimpleDB stores field names in lower case
			int a1 = scan_a.getInt("a1");
			int a2 = scan_a.getInt("a2");
			System.out.println(a1 + "\t" + a2);
		}
		scan_a.close();
		tx.commit();
		
		tx = new Transaction();
		Scan scan_b = SimpleDB.planner().createQueryPlan(query_b, tx).open();
		System.out.println("\nB1\tB2");
		while (scan_b.next()) {
			//SimpleDB stores field names in lower case
			int b1 = scan_b.getInt("b1");
			String b2 = scan_b.getString("b2");
			System.out.println(b1 + "\t" + b2);
		}
		scan_a.close();
		tx.commit();
	}
}
