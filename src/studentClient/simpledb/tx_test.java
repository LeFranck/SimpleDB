package studentClient.simpledb;
import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.server.SimpleDB;


public class tx_test {
	
		public static void main(String[] args) {
			try {
				// analogous to the driver
				SimpleDB.init("testdb0");
				
				// analogous to the connection
				Transaction tx = new Transaction();
				String qry = "insert into STUDENT(SId, SName, MajorId, GradYear) values"
				        + "(98949, 'WUTy', 10, 2018)";	

		        int result = SimpleDB.planner().executeUpdate(qry, tx);
				
				// analogous to the statement

				
				tx.commit();
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}

}
