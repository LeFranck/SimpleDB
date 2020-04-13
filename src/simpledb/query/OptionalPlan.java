package simpledb.query;

import simpledb.record.Schema;
import simpledb.record.TableInfo;

public class OptionalPlan implements Plan {
	private Plan p1, p2;
	private Schema schema = new Schema();
	private String fldname1, fldname2;
	private TableInfo tr;
	
	
	public OptionalPlan(Plan p1, Plan p2, String fldname1, String fldname2, TableInfo tr)
	{
		this.p1 = p1;
	    this.p2 = p2;
	    this.fldname1 = fldname1;
	    this.fldname2 = fldname2;
	    this.tr = tr;
	    schema.addAll(p1.schema());
	    schema.addAll(p2.schema());
	}
	
	
	//TableInfo de la relacion de la derecha
	@Override
	public Scan open() {
	      Scan s1 = p1.open();
	      Scan s2 = p2.open();
	      return new OptionalScan(s1, s2, fldname1, fldname2, tr);
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
	      return schema;
	}



}
