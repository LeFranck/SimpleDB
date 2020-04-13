package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class TransitiveScan implements Scan {
	private Scan s;
	private String fldname1, fldname2;
	
	public TransitiveScan(Scan s, String fldname1, String fldname2)
	{
		this.s = s;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
	}
	
	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		return s.next();
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public Constant getVal(String fldname) {
		return s.getVal(fldname);
	}

	@Override
	public int getInt(String fldname) {
		return s.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		return s.getString(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return s.hasField(fldname);
	}

}
