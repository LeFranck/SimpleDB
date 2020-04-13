package studentClient.simpledb;


import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateDBWithException {
	public static void main(String[] args) throws Exception {
		SimpleDB.init("lab4_test1");
		
		createTables();
		insertData();
	}
	
	public static void createTables() {
		Transaction tx = new Transaction();
		String s1 = "create table a(a1 int, a2 int)";
		String s2 = "create table b(b1 int, b2 varchar(10))";
		
		SimpleDB.planner().executeUpdate(s1, tx);
		SimpleDB.planner().executeUpdate(s2, tx);

		tx.commit();
		System.out.println("Tables created.");
	}
	
	public static void insertData() throws Exception {
		
		String insert_a = "insert into a(a1, a2) values ";
		String insert_b = "insert into b(b1, b2) values ";
		
		Transaction tx1 = new Transaction();
		Transaction tx2 = new Transaction();
		Transaction tx3 = new Transaction();
		Transaction tx4 = new Transaction();
		
		//transacción 1
		for (int i=0; i < 20; i++)
			SimpleDB.planner().executeUpdate(insert_a + "("+i+", "+i+")", tx1);
		tx1.commit();
		
		//transacción 2
		for (int i=20; i < 40; i++)
			SimpleDB.planner().executeUpdate(insert_a + "("+i+", "+i*i+")", tx2);
		//t2 unfinished

		//transacción 3
		for (int i=0; i < 20; i++)
			SimpleDB.planner().executeUpdate(insert_b + "("+i+", 'str"+i+"')", tx3);
		tx3.commit();
		
		//transacción 4
		for (int i=20; i < 40; i++)
			SimpleDB.planner().executeUpdate(insert_b + "("+i+", 'str"+i*i+"')", tx4);
		
		//tx4 unfinished
		throw new Exception();
	}
}
