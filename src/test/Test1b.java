package test;

import java.util.Map;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.IndexMgr;
import simpledb.metadata.TableMgr;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Test1b {
	/**
	 * Test 1: Insert y query con llaves enteras
	 **/

	private static int BLOCK_SIZE = 400;
	private static int INT_SIZE = 4;
	private static int INITIAL_BUCKETS = 2; // TODO: cambiar
	private static int TUPLES_PER_BLOCK = BLOCK_SIZE/(INT_SIZE * 4); //RID(2)+ IN_USE + KEY
	
	private static int SIZE = TUPLES_PER_BLOCK * 2;
	
    private static IndexMgr idxmgr;
    private static TableMgr tblMgr;
    private static Transaction tx;
    
    private static final String idxname = "indext1b";
    private static final String tblname = "tablet1b";
    private static final String fldname = "id";
    private static final String serverDirectory = "testdb";
	
    public static void initializeDatabaseServer()
    {
        Schema sch = new Schema();
        sch.addIntField("id");
        sch.addStringField("fld1", TableMgr.MAX_NAME);
        sch.addStringField("fld2", TableMgr.MAX_NAME);

        SimpleDB.initFileLogAndBufferMgr(serverDirectory);
        tx = new Transaction();
        tblMgr = new TableMgr(true, tx);
        idxmgr = new IndexMgr(true, tblMgr, tx);
        tblMgr.createTable(tblname, sch, tx);
        idxmgr.createIndex(idxname, tblname, fldname, tx);

        SimpleDB.initMetadataMgr(true, tx);
    }
    
    public static void main(String[] args) {    	
    	System.out.println("Test 1b:");
    	System.out.print("Initializing database...");
    	initializeDatabaseServer();
    	System.out.println("ready");
    	Map<String, IndexInfo> indexes = SimpleDB.mdMgr().getIndexInfo(tblname, tx);
    	IndexInfo ii = indexes.get(fldname);
        Index idx = ii.open();
        System.out.print("Inserting data...");
        insertData(idx);
        System.out.println("ready");
        queryData(idx);
    	idx.close();
    }
    
    private static void insertData(Index idx) {
    	for (int i = 0; i < SIZE*INITIAL_BUCKETS; i += INITIAL_BUCKETS) {
    		Constant key = new IntConstant(i);
    		RID rid = new RID(i/10, i%10); //blknum,id
    		idx.insert(key, rid);
    		if (i%4 == 0) {// multiplos de 4, inserto 3 veces
    			idx.insert(key, rid);
    			idx.insert(key, rid);
    		}
    	}
    }
    
    private static void queryData(Index idx) {
    	for (int i = 0; i < SIZE*INITIAL_BUCKETS; i+=INITIAL_BUCKETS/2) {
    		Constant key = new IntConstant(i);
    		idx.beforeFirst(key);
    		System.out.println("key: " + i);
    		while (idx.next()) {
    			RID val = idx.getDataRid();
    			System.out.println("\t(" + val.blockNumber() + ", " + val.id() + ")");
    		}
    	}
    }
}