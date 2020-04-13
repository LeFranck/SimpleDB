package studentClient.simpledb;
import java.sql.*;

import simpledb.index.Index;
import simpledb.index.planner.IndexUpdatePlanner;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.IndexMgr;
import simpledb.metadata.TableMgr;
import simpledb.parse.InsertData;
import simpledb.parse.Parser;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.beans.Statement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class pruebalab2_3 {

    private static IndexMgr idxmgr;

    private static TableMgr tblMgr;

    private static Transaction tx;

    private static final String idxname = "indexpruebagente";
    private static final String tblname = "gente";
    private static final String fldname = "id";
    private static final String serverDirectory = "testdb0";
	
	public static void main(String[] args) {
		
		Schema sch = new Schema();
	    sch.addIntField("id");
	    sch.addStringField("name", TableMgr.MAX_NAME);
	    sch.addStringField("nickname", TableMgr.MAX_NAME);

	    SimpleDB.initFileLogAndBufferMgr(serverDirectory);
	    tx = new Transaction();
	    tblMgr = new TableMgr(true, tx);
	    idxmgr = new IndexMgr(true, tblMgr, tx);
	    tblMgr.createTable(tblname, sch, tx);
	    idxmgr.createIndex(idxname, tblname, fldname, tx);
	    SimpleDB.initMetadataMgr(true, tx);
	    tx.commit();

	    for(int i = 0; i < 320; i++)
	    {
	    	//BUGS
	    	int k = 1;
	    	if(i > 301){
	    		System.out.print("hola");
	    		k = 2;
	    	}
	        String s = "insert into "+idxname+"(id, name, nickname) values ("+i+", 'joe"+i+"', 'loco') ";
	        Parser parser = new Parser(s);
	        Object obj = parser.updateCmd();

	        Plan p = new TablePlan(tblname, tx);

	        
	        // first, insert the record
	        UpdateScan scan = (UpdateScan) p.open();
	        scan.insert();
	        RID rid = scan.getRid();

	        // then modify each field, inserting an index record if appropriate
	        // se buscara en indexmgr todos los indices relacionados con la tabla tblname y se devolveran
	        // <fieldname, indexinfo>
	        Map<String, IndexInfo> indexes = SimpleDB.mdMgr().getIndexInfo(tblname, tx);

	        Iterator<Constant> valIter = ((InsertData) obj).vals().iterator();
	        for (String dataFldname : ((InsertData) obj).fields())
	        {
	            Constant val = valIter.next();
	            System.out.println("Modify field " + dataFldname + " to val " + val);
	            scan.setVal(dataFldname, val);

	            IndexInfo ii = indexes.get(dataFldname);
	            if (ii != null)
	            {
	            	System.out.println(dataFldname);
	                Index idx = ii.open();
	                //PROBLEMAS EN EL INSERT --> setINT o get INT
	                idx.insert(val, rid);
	                idx.close();
	            }
	        }
	        scan.close();
	    }
	
	
	}
	
}