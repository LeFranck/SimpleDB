package Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import simpledb.index.query.IndexJoinPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.opt.HeuristicQueryPlanner;
import simpledb.opt.SelingerPlanner;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.planner.BasicQueryPlanner;
import simpledb.query.Plan;
import simpledb.query.Predicate;
import simpledb.query.SelectPlan;
import simpledb.query.TablePlan;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class T3 {

    public static void main(String[] args) {
		//String major = args[0];
		String major = "math";
		System.out.println("Here are the " + major + " majors");
		System.out.println("Name\tGradYear");

		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			SimpleDB.init("testdb0");
			Transaction tx = new Transaction();
			
			Plan p1 = new TablePlan("student",tx);
			
			String qry = "select sname, gradyear "
			           + "from student, dept "
			           + "where did = majorid "
			           + "and dname = '" + major + "'";
			Parser p = new Parser(qry);
			QueryData data = p.query();
			Predicate joepred = data.pred();
			
//			BasicQueryPlanner basic = new BasicQueryPlanner();
//			Plan bpaln = basic.createPlan(data, tx);
			SelingerPlanner selinger = new SelingerPlanner();
			Plan p_selinger = selinger.createPlan(data, tx);

			HeuristicQueryPlanner heu = new HeuristicQueryPlanner();
			Plan p5 = heu.createPlan(data, tx);

			System.out.println("WUT");
//			Plan p2 = new SelectPlan(p1,joepred);
//			
//			Plan p3 = new TablePlan("enroll",tx);
//			
//			//indexjoin
//			MetadataMgr mdMgr = SimpleDB.mdMgr();
//			Map<String, IndexInfo> indexes = mdMgr.getIndexInfo("enroll", tx);
//			IndexInfo ii = indexes.get("studentid");
//			Plan p4 = new IndexJoinPlan(p2,p3,ii,"sid",tx);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			// Step 4: close the connection
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
