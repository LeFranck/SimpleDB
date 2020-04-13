package studentClient.simpledb;

import simpledb.metadata.MetadataMgr;
import simpledb.query.Constant;
import simpledb.query.OptionalPlan;
import simpledb.query.OptionalScan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Optional_prueba {

	private static TableInfo sti;
	private static TableInfo bti;
	private static Scan s;
	private static Scan s1;
	private static Scan s2;
	private static Scan s3;

	
	public static void main(String[] args) {

		SimpleDB.init("wooola1");
		Transaction tx = new Transaction();
		MetadataMgr mdmgr = SimpleDB.mdMgr();
		sti = mdmgr.getTableInfo("student", tx);
		bti = mdmgr.getTableInfo("blog", tx);
		TablePlan p1 = new TablePlan("student", tx);
		TablePlan p2 = new TablePlan("blog", tx);
		String fldname1 = "sid";
		String fldname2 = "sid";
		String fldname3 = "bname";
		OptionalPlan p = new OptionalPlan(p1, p2, fldname1, fldname2, bti);
		s = (OptionalScan) p.open();
		//s1 = new TableScan(sti, tx);
		//s2 = new TableScan(bti, tx);

		//s3 = new OptionalScan(s1,s2,fldname1,fldname2, bti);
		while(s.next())
		{
			print_row();
		}
		
		
	}
	
	public static void print_row()
	{
		Schema scht1 = sti.schema();
		Schema scht2 = bti.schema();
		
		for (String fldname : scht1.fields()) 
		{
			System.out.print(fldname + "\t");
			Constant a = s.getVal(fldname);
			System.out.println(s.getVal(fldname) + "\t");
		}
		
		for (String fldname : scht2.fields()) 
		{
			System.out.print(fldname + "\t");
			System.out.println(s.getVal(fldname) + "\t");
		}
		System.out.println("_______________");
		
	}

}
