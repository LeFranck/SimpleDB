package studentClient.simpledb;

import simpledb.metadata.MetadataMgr;
import simpledb.materialize.TransitiveScan;
import simpledb.materialize.TransitivePlan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Transitive_prueba {

	private static TableInfo sti;
	private static Scan s;
	private static TablePlan p;

	
	public static void main(String[] args) {

		SimpleDB.init("wooola1");
		Transaction tx = new Transaction();
//		MetadataMgr mdmgr = SimpleDB.mdMgr();
//		sti = mdmgr.getTableInfo("trips", tx);
//		s = new TableScan(sti, tx);
		p = new TablePlan("trip", tx);
		TransitivePlan tp = new TransitivePlan(p, "origen", "destino", tx);
		s = new TransitiveScan(tp.open(), "origen", "destino");
		s.beforeFirst();
		while(s.next())
		{
			print_row();
		}
		
		
	}
	
	public static void print_row()
	{
		System.out.print("Origen" + "\t");
		System.out.println(s.getVal("origen") + "\t");
		
		System.out.print("Destino" + "\t");
		System.out.println(s.getVal("destino") + "\t");
		System.out.println("_______________");
		
		System.out.println("_______________");
		
	}

}