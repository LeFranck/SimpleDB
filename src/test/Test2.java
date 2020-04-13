package test;

import java.util.Map;
import java.util.Random;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.IndexMgr;
import simpledb.metadata.TableMgr;
import simpledb.query.Constant;
import simpledb.query.StringConstant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Test2 {
	/**
	 * Test 2: Insert y query con llaves string
	 **/

    private static IndexMgr idxmgr;
    private static TableMgr tblMgr;
    private static Transaction tx;
    
    private static final String idxname = "indext2";
    private static final String tblname = "tablet2";
    private static final String fldname = "fld1";
    private static final String serverDirectory = "testdb";
    private static Random r = new Random();
    private static int SIZE = 200;
	
    public static void initializeDatabaseServer()
    {
        Schema sch = new Schema();
        sch.addIntField("id");
        sch.addStringField("fld1", 7);
        sch.addStringField("fld2", TableMgr.MAX_NAME);

        SimpleDB.initFileLogAndBufferMgr(serverDirectory);
        tx = new Transaction();
        tblMgr = new TableMgr(true, tx);
        idxmgr = new IndexMgr(true, tblMgr, tx);
        tblMgr.createTable(tblname, sch, tx);
        idxmgr.createIndex(idxname, tblname, fldname, tx);

        SimpleDB.initMetadataMgr(true, tx);
    }
    
    public static void main(String[] args){
    	System.out.println("Test 2:");
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
    	for (int i = 0; i < SIZE; i+=2) {
    		Constant key = new StringConstant("str" + i + "X");
    		RID rid = new RID(i/10, i%10); //blknum,id
    		idx.insert(key, rid);
    		if (i%4 == 0) {// multiplos de 4, inserto 3 veces
    			idx.insert(key, rid);
    			idx.insert(key, rid);
    		}
    	}
    }
    
    private static void queryData(Index idx) {
    	for (int i = 0; i < SIZE; i+=3) {
    		Constant key = new StringConstant("str" + i + "X");
    		idx.beforeFirst(key);
    		System.out.println("key: " + key.toString());
    		while (idx.next()) {
    			RID val = idx.getDataRid();
    			System.out.println("\t(" + val.blockNumber() + ", " + val.id() + ")");
    		}
    	}
    }
    
    private static String randomString(final int length){
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder salt = new StringBuilder();
        while (salt.length() < length) {
            int index = (int) (r.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }
}

