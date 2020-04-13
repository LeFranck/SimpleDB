package simpledb.index.hash;

import java.io.IOException;
//import java.io.PrintWriter;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Edward Sciore
 */
public class HashIndex implements Index {
	public static int NUM_BUCKETS = 100;
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;
	private TableScan tsDir = null;
	private TableInfo hashDirTi;
	private Schema schHashDir;
	private int pisos;
	private int numBuckets;

	/**
	 * Opens a hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	//EDITADO PARA FIJAR EL PRIMER NIVEL
	public HashIndex(String idxname, Schema sch, Transaction tx) {
		//debug_file();

		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;

		//Aparte de agregar el index a index cat se debe crear idxnameDIR y los dos primeros buckets

		//idxnameDIR
		//identificador tiene largo maximo 7, debido a que hay 100 buckets a usar
		// en 7 se pueden llegar a usar hasta 128 buskcets sin overflowpages
		
		String indexDirTable  = idxname + "dir";
		schHashDir = new Schema();
        schHashDir.addStringField("identificador", 7);
        schHashDir.addIntField("numbucket");
        schHashDir.addIntField("profundidad");
		hashDirTi = new TableInfo(indexDirTable, schHashDir);
		//Si es nueva, se deben agregar los dos primeros id y los dos primeros buckets
		if (tx.size(hashDirTi.fileName()) == 0)
		{
        	tx.append(hashDirTi.fileName(), new HashPageFormatter(hashDirTi, 1, 2));
			tsDir = new TableScan(hashDirTi, tx);
			tsDir.beforeFirst();
			//record = (0,0,1)
			tsDir.insert();
			tsDir.setString("identificador", "0");
			tsDir.setInt("numbucket", 0);
			tsDir.setInt("profundidad", 1);
			String tblname0 = idxname + 0;
			TableInfo ti0 = new TableInfo(tblname0, sch);
			ts = new TableScan(ti0, tx);
			ts.close();
			
			//record = (1,1,1)
			tsDir.insert();
			tsDir.setString("identificador", "1");
			tsDir.setInt("numbucket", 1);
			tsDir.setInt("profundidad", 1);
			
			String tblname1 = idxname + 1;
			TableInfo ti1 = new TableInfo(tblname1, sch);
			ts = new TableScan(ti1, tx);
			ts.close();
			
			tsDir.close();
			
			
			
		}
		tsDir = new TableScan(hashDirTi, tx);
		tsDir.beforeFirst();
		tsDir.next();
		pisos = tsDir.getInt("numbucket");
		numBuckets = tsDir.getInt("profundidad");
	}

	/**
	 * Positions the index before the first index record
	 * having the specified search key.
	 * The method hashes the search key to determine the bucket,
	 * and then opens a table scan on the file
	 * corresponding to the bucket.
	 * The table scan for the previous bucket (if any) is closed.
	 * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		String identificador = binhash(searchkey, pisos);
		//Estara en orden ascendente IdxDIR, y tendra un primer record de header
		int posicion = Integer.parseInt(identificador, 2);
		
		
		//CAMBIAR 0 por el bloque donde estara la posicion 
		RID aux = new RID(0,posicion + 1);
		
		tsDir.moveToRid(aux);
		int bucket = tsDir.getInt("numbucket");
		//int bucket = searchkey.hashCode() % NUM_BUCKETS;
		String tblname = idxname + bucket;
		TableInfo ti = new TableInfo(tblname, sch);
		
		//CREA LA TABLA H1
		ts = new TableScan(ti, tx);
	}

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see simpledb.index.Index#next()
	 */
	public boolean next() {
		while (ts.next())
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}

	/**
	 * Retrieves the dataRID from the current record
	 * in the table scan for the bucket.
	 * @see simpledb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		//CABE?
		//IF CABE == TRUE
		boolean haveSpace = ts.PageHasSpaceForRecord();
		//get have space ts
		if(haveSpace)
		{
			//cambiar el flag to inuse
			//Lo hace insert por abajo
			beforeFirst(val);
			ts.insert();
			ts.setInt("block", rid.blockNumber());
			ts.setInt("id", rid.id());
			ts.setVal("dataval", val);
		}else{
			ExpandHash(val, rid);
			System.out.println("DEBERIA EXPANDER EL HASH");
		}
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while(next())
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	public void close() {
		if (ts != null)
			ts.close();
	}

	/**
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb){
		return numblocks / HashIndex.NUM_BUCKETS;
	}
	
	private String binhash(Constant key, int pisos)
	{
		//Se puede aplicar modulo 2^n
		int hc = key.hashCode();
		String binary = Integer.toBinaryString(hc);
		return binary.substring(binary.length()-pisos);
	}
	
	private void ExpandHash(Constant val, RID rid)
	{
		//Evaluar profundidad del bucket
		//if profundidad < profundidad global
			//Create new bucket and conect
			//ExpandBucket(Constant val, RID rid);
			
			//ME GUSTA MAS 
			//ExpandBucket(string identificador, int numbucket);
			//insert(Constant val, RID rid);
		//else
			//ExpandTable(Constant val, RID rid);
	}
	
	
}
