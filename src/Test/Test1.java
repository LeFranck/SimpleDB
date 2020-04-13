package Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import simpledb.materialize.SortPlan;
import simpledb.opt.HeuristicQueryPlanner;
import simpledb.opt.SelingerPlanner;
import simpledb.planner.BasicQueryPlanner;
import simpledb.planner.BasicUpdatePlanner;
import simpledb.planner.Planner;
import simpledb.planner.UpdatePlanner;
import simpledb.query.Plan;
import simpledb.query.Predicate;
import simpledb.query.ProjectPlan;
import simpledb.query.Scan;
import simpledb.query.SelectPlan;
import simpledb.query.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Test1 {
	public static void main(String[] args) {
		SimpleDB.init("test1Lab4DBg");
		
		Transaction tx = new Transaction();
		insertData(tx);
		tx.commit();
				
		tx = new Transaction();
		HeuristicQueryPlanner greedy = new HeuristicQueryPlanner();
		SelingerPlanner custom = new SelingerPlanner();//TODO: cambiar por el implementado
		UpdatePlanner uplanner = new BasicUpdatePlanner();
		Planner greedyPlanner = new Planner(greedy, uplanner);
		Planner customPlanner = new Planner(custom, uplanner);
		
		String query = "select fld1, fld2, fld3, fld4, fld5 from table1, table2, table3, table4, table5 where fld1 = fld2 and fldb = fldc " +
		"and fld3 = fld4 and fldd = flde";
					
		Plan planGreedy = greedyPlanner.createQueryPlan(query, tx);
		System.out.println("Estimated cost greedy: " + planGreedy.recordsOutput());
		Plan planCustom = customPlanner.createQueryPlan(query, tx);
		System.out.println("Estimated cost custom: " + planCustom.recordsOutput());
		Scan s = planCustom.open();
					
		System.out.println("fld1\tfld2\tfld3\tfld4\tfld5");
		while (s.next()) {
			//SimpleDB stores field names in lower case
			int s1 = s.getInt("fld1");
			int s2 = s.getInt("fld2");
			int s3 = s.getInt("fld3");
			int s4 = s.getInt("fld4");
			int s5 = s.getInt("fld5");
			System.out.println(s1 + "\t" + s2 + "\t" + s3 + "\t" + s4 + "\t" + s5);
		}
		s.close();
		tx.commit();

	}
	
	public static void insertData(Transaction tx) {
		String s1 = "create table table1(fld1 int, flda varchar(10))";
		String s2 = "create table table2(fldb varchar(10), fld2 int)";
		String s3 = "create table table3(fld3 int, fldc varchar(10))";
		String s4 = "create table table4(fldd varchar(10), fld4 int)";
		String s5 = "create table table5(fld5 int, flde varchar(10))";

		SimpleDB.planner().executeUpdate(s1, tx);
		SimpleDB.planner().executeUpdate(s2, tx);
		SimpleDB.planner().executeUpdate(s3, tx);
		SimpleDB.planner().executeUpdate(s4, tx);
		SimpleDB.planner().executeUpdate(s5, tx);
		System.out.println("Tables created.");
		
		String insert1 = "insert into table1(fld1, flda) values ";
		String insert2 = "insert into table2(fld2, fldb) values ";
		String insert3 = "insert into table3(fld3, fldc) values ";
		String insert4 = "insert into table4(fld4, fldd) values ";
		String insert5 = "insert into table5(fld5, flde) values ";
		
		for (int i=0; i < 20; i++)
			SimpleDB.planner().executeUpdate(insert1 + "(" + i + ", 'str" + i + "')", tx);
		for (int i=0; i < 30; i++)
			SimpleDB.planner().executeUpdate(insert2 + "(" + i + ", 'str" + i + "')", tx);
		for (int i=0; i < 50; i++)
			SimpleDB.planner().executeUpdate(insert3 + "(" + i + ", 'str" + i + "')", tx);
		for (int i=0; i < 100; i++)
			SimpleDB.planner().executeUpdate(insert4 + "(" + i + ", 'str" + i + "')", tx);
		for (int i=0; i < 80; i++)
			SimpleDB.planner().executeUpdate(insert5 + "(" + i + ", 'str" + i + "')", tx);

		System.out.println("Data inserted.");
	}
}
