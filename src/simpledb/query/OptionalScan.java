package simpledb.query;

import simpledb.record.TableInfo;
import static java.sql.Types.INTEGER;


public class OptionalScan implements Scan {
	private Scan s1, s2;
	private String fldname1, fldname2;
	private TableInfo tr;
	private boolean s1_in_s2;
	private boolean return_s1_empty_s2;
	private int int_null = (int) (Math.pow(2, 31) - 1);
	
	public OptionalScan(Scan s1, Scan s2,  String fldname1, String fldname2, TableInfo tr) {
	      this.s1 = s1;
	      this.s2 = s2;
	      this.fldname1 = fldname1;
	      this.fldname2 = fldname2;
	      s1_in_s2 = false;
	      this.tr = tr;
	      return_s1_empty_s2 = false;
	      beforeFirst();
	}
	
	@Override
	public void beforeFirst() {
	      s1.beforeFirst();
	      s1.next();
	      s2.beforeFirst();
	}

	//Buscara en s2 alguna coinciddencia de campos
	@Override
	public boolean next() {
		if(return_s1_empty_s2)
		{
			if(s1.next())
			{
				return_s1_empty_s2 = false;
				s2.beforeFirst();
			}
			else
				return false;
		}
		while(s2.next())
		{
			Constant id1 = s1.getVal(fldname1);
			Constant id2 = s2.getVal(fldname2);
			if( s1.getVal(fldname1).equals(s2.getVal(fldname2)) )
			{
				s1_in_s2 = true;
				return true;
			}
		}
		if(s1_in_s2)
		{
			if(s1.next())
			{
				s1_in_s2 = false;
				s2.beforeFirst();
				return next();
			}
			return false;
		}
		return_s1_empty_s2 = true;
		return true;
	}

	@Override
	public void close() {
	      s1.close();
	      s2.close();
	}

	@Override
	public Constant getVal(String fldname) {
	      if (s1.hasField(fldname))
	          return s1.getVal(fldname);
	       else if (s2.hasField(fldname)  && s1_in_s2)
	          return s2.getVal(fldname);
	       else if (s2.hasField(fldname) && !s1_in_s2)
	       {
	    	   if(tr.schema().type(fldname) == INTEGER)
	    	   {
	    	         return new IntConstant(getInt(fldname));
	    	   }else{
	    	         return new StringConstant(getString(fldname));
	    	   }
	       }
	       else
	    	   return null;
	}

	@Override
	public int getInt(String fldname) {
	      if (s1.hasField(fldname))
	          return s1.getInt(fldname);
	       else if (s2.hasField(fldname) && s1_in_s2) 
	          return s2.getInt(fldname);
	       else if (s2.hasField(fldname) && !s1_in_s2)
	    	   return int_null;
	       else
	       { 
	    	   System.out.println("El campo no existe en dicha tabla");
	    	   return int_null;
	       }
	}

	@Override
	public String getString(String fldname) {
	      if (s1.hasField(fldname))
	          return s1.getString(fldname);
	       else if (s2.hasField(fldname)  && s1_in_s2) 
	          return s2.getString(fldname);
	       else if (s2.hasField(fldname) && !s1_in_s2)
	    	   return "NULL";
	       else
	       {
	    	   System.out.println("El campo no existe en dicha tabla");
	    	   return "NULL";
	       }
	}

	@Override
	public boolean hasField(String fldname) {
	      return s1.hasField(fldname) || s2.hasField(fldname);
	}

}
