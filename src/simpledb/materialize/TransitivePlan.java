package simpledb.materialize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.ProjectPlan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class TransitivePlan implements Plan {
	private Transaction tx;
	private Plan p;
	private ProjectPlan pp;
	private Schema schema;
	private Collection<String> fieldlist = new ArrayList<String>();
	private String fldname1, fldname2;
	private Scan pScan;
	private UpdateScan fin , scanFin;
	
	public TransitivePlan(Plan p, String fldname1, String fldname2, Transaction tx) {
		this.p = p;
		this.tx = tx;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		fieldlist.add(fldname1);
		fieldlist.add(fldname2);
		schema = new Schema();
		schema.addAll(p.schema());
//		for (String fldname : fieldlist)
//			schema.add(fldname, p.schema());
//	    for (String fldname : p.schema().fields())
//	    	schema.add(fldname, p.schema());
	}
	
	
	@Override
	public Scan open() {
		pp = new ProjectPlan(p, fieldlist);
		pScan = pp.open();
		fillTempTable();
		return fin;
	}

	public void fillTempTable()
	{
		boolean corre = true;
		TempTable tempTable = new TempTable(pp.schema(), tx);
		fin = tempTable.open();
		transferScanToScan(pScan, fin, pp.schema());
		while(corre)
		{
			TempTable tempTable1 = new TempTable(pp.schema(), tx);
			TempTable tempTable2 = new TempTable(pp.schema(), tx);
			UpdateScan upTempScan = tempTable1.open();
			UpdateScan tempAux = tempTable1.open();
			transferScanToScan(fin, upTempScan, pp.schema());
			transferScanToScan(fin, tempAux, pp.schema());
			corre = addInfo(upTempScan, tempAux);
		}
	}
	
	private boolean addInfo(UpdateScan upTempScan, UpdateScan tempAux)
	{
		pScan.beforeFirst();
		upTempScan.beforeFirst();
		tempAux.beforeFirst();
		boolean hasAdded = false;
		while(pScan.next())
		{
			//original	Conocidos  Conocidos y agregados
			// [a,b] 	 [c,d]  		[e,f]
			boolean alreadyExist = false;
			Constant a = pScan.getVal(fldname1);
			Constant b = pScan.getVal(fldname2);
			while(upTempScan.next())
			{
				Constant c = upTempScan.getVal(fldname1);
				Constant d = upTempScan.getVal(fldname2);
				if(b.equals(c))
				{
					while(tempAux.next())
					{
						Constant e = tempAux.getVal(fldname1);
						Constant f = tempAux.getVal(fldname2);
						if(a.equals(e) && d.equals(f))
						{
							alreadyExist = true;
						}
					}
					if(!alreadyExist)
					{
						tempAux.insert();
						tempAux.setVal(fldname1, a);
						tempAux.setVal(fldname2, d);
						fin.insert();
						fin.setVal(fldname1, a);
						fin.setVal(fldname2, d);
						hasAdded = true;
						System.out.print("Origen" + "\t");
						System.out.println(a + "\t");
						
						System.out.print("Destino" + "\t");
						System.out.println(d + "\t");
						System.out.println("_______________");
						
						System.out.println("_______________");
					}
					tempAux.beforeFirst();
				}
			}
			upTempScan.beforeFirst();
		}
		return hasAdded;
	}
	
	private void transferScanToScan(Scan s, UpdateScan up, Schema sch)
	{
		//Copiar la original
		s.beforeFirst();
		while(s.next())
		{
			up.insert();
			for (String fldname : sch.fields())
				up.setVal(fldname, s.getVal(fldname));
		}
		up.beforeFirst();
		s.beforeFirst();
	}
	
	@Override
	public int blocksAccessed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int recordsOutput() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int distinctValues(String fldname) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Schema schema() {
		// TODO Auto-generated method stub
		return null;
	}

}
