package simpledb.index.hash;

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
	private TableScan tsnewb = null;
	private TableInfo hashDirTi;
	private Schema schHashDir;
	private int pisos_global;
	private int bucketnumbers;
	
	private int bucket_insert;
	private int profundidad_local;
	private String identificador;
	
	private String dir1 = "identificador"; 
	private String dir2 = "bucketnumber"; 
	private String dir3 = "profundidad"; 

	/**
	 * Opens a hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	//EDITADO PARA FIJAR EL PRIMER NIVEL
	public HashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;

		//Aparte de agregar el index a index cat se debe crear idxnameDIR y los dos primeros buckets

		//idxnameDIR
		//identificador tiene largo maximo 7, debido a que hay 100 buckets a usar
		// en 7 se pueden llegar a usar hasta 128 buskcets sin overflowpages
		
		String indexDirTable  = idxname + "dir";
		schHashDir = new Schema();
        schHashDir.addStringField(dir1, 7);
        schHashDir.addIntField(dir2);
        schHashDir.addIntField(dir3);
		hashDirTi = new TableInfo(indexDirTable, schHashDir);
		//Si es nueva, se deben agregar los dos primeros id y los dos primeros buckets
		if (tx.size(hashDirTi.fileName()) == 0)
		{
        	tx.append(hashDirTi.fileName(), new HashPageFormatter(hashDirTi, 1, 2));
			tsDir = new TableScan(hashDirTi, tx);
			tsDir.beforeFirst();
			//record = (0,0,1)
			tsDir.insert();
			tsDir.setString(dir1, "0");
			tsDir.setInt(dir2, 0);
			tsDir.setInt(dir3, 1);
			String tblname0 = idxname + 0;
			TableInfo ti0 = new TableInfo(tblname0, sch);
			ts = new TableScan(ti0, tx);
			ts.close();
			
			//record = (1,1,1)
			tsDir.insert();
			tsDir.setString(dir1, "1");
			tsDir.setInt(dir2, 1);
			tsDir.setInt(dir3, 1);
			
			String tblname1 = idxname + 1;
			TableInfo ti1 = new TableInfo(tblname1, sch);
			ts = new TableScan(ti1, tx);
			ts.close();
			
			tsDir.close();
			
			
			
		}
		tsDir = new TableScan(hashDirTi, tx);
		tsDir.beforeFirst();
		tsDir.next();
		pisos_global = tsDir.getInt(dir2);
		bucketnumbers = tsDir.getInt(dir3);
		
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
		identificador = binhash(searchkey, pisos_global);
		
		//Estara en orden ascendente IdxDIR, y tendra un primer record de header
		int num_identificador = Integer.parseInt(identificador, 2);
		int bloque = Math.floorDiv(num_identificador + 1,17);
		int rid = (num_identificador+1)%17;
		
		//AMBIGUEDAD, posicion se refiere al id dentro del bloque o al id en la tabla?
		//Se considera como el id en la tabla en esta ocacion
		//RID aux = new RID(bloque,posicion + 1);

		//id de la tabla
		RID aux = new RID(bloque,rid);

		
		tsDir.moveToRid(aux);
		bucket_insert = tsDir.getInt(dir2);
		profundidad_local = tsDir.getInt(dir3);
		//int bucket = searchkey.hashCode() % NUM_BUCKETS;
		String tblname = idxname + bucket_insert;
		TableInfo ti = new TableInfo(tblname, sch);
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
		beforeFirst(val);
		boolean haveSpace = ts.PageHasSpaceForRecord();
		if(haveSpace){
			ts.insert();
			ts.setInt("block", rid.blockNumber());
			ts.setInt("id", rid.id());
			ts.setVal("dataval", val);
		}else{
			
			if(profundidad_local == pisos_global){
				ExtendHash();
			}else{
				ExtendBucket();
			}
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
		if (tsDir != null)
			tsDir.close();
		if (tsnewb != null)
			tsnewb.close();
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
		//Caso borde
		while(binary.length() < pisos)
		{
			binary = "0"+binary;
		}
		return binary.substring(binary.length()-pisos);
	}
	
	private void ExtendHash()
	{
		tsDir.beforeFirst();
		tsDir.next();
		int num_directorios = (int) Math.pow(2, pisos_global);
		ExtendHashEditDir(num_directorios);

		//Buscar bucket a editar(bucket insert)
		RID aux = getBucketaEditar(bucket_insert);
		//int bn = Math.floorDiv(bucket_insert+1,17);
		//int id = (bucket_insert+1)%17;
		//RID aux = new RID(bn,id);
		tsDir.moveToRid(aux);
		//int bucket = tsDir.getInt(dir2);
		//int profundidad = tsDir.getInt(dir3);
		old_bucket();
		//String idxbucketinsert = idxname + bucket_insert;
		//TableInfo tbinsert = new TableInfo(idxbucketinsert, sch);
		//ts = new TableScan(tbinsert, tx);
		


		//Genera una nuevo bucket-->tabla
		//20 en torno al debug
		new_bucket();
		//String idxnewbucket = idxname + bucketnumbers;
		//TableInfo tbnew = new TableInfo(idxnewbucket, sch);
		//tsnewb = new TableScan(tbnew, tx);


		//editar num pisos
		pisos_global++;
		//recorrer el bucket insert y pegar en la nueva tabla todo lo que al hacer hashkey de el nuevo identificador
		//eliminar del bucket insert todo lo que se agrego a la nueva tabla(misma ejecucion)

		while(ts.next())
		{
			int i1 = ts.getInt("block");
			int i2 = ts.getInt("id");
			Constant val = ts.getVal("dataval");
			String newkeyhash = binhash(val, pisos_global);

			if(newkeyhash.charAt(0) == '1'){
				transfer_record(i1, i2, val);
				//tsnewb.insert();
				//tsnewb.setInt("block", i1);
				//tsnewb.setInt("id", i2);
				//tsnewb.setVal("dataval", val);
				//ts.delete();
			}
		}
		
		//asignar cableado del nuevo bucket
		String auxdir = "1"+identificador;
		int auxdirint = Integer.parseInt(auxdir,2); 
		int auxbn = Math.floorDiv(auxdirint+1,17);
		int auxid = (auxdirint+1)%17;
		RID auxrid = new RID(auxbn,auxid);
		tsDir.moveToRid(auxrid);
		tsDir.setString(dir1, auxdir);
		tsDir.setInt(dir2, bucketnumbers);
		tsDir.setInt(dir3, pisos_global);
		//subir profundidad local del bucket que se repartio
		tsDir.moveToRid(aux);
		tsDir.setInt(dir3, pisos_global);
		
		
		//num buckets
		bucketnumbers++;
		tsDir.beforeFirst();
		tsDir.next();
		tsDir.setInt(dir2, pisos_global);
		tsDir.setInt(dir3, bucketnumbers);
	}
	

	
	private void ExtendBucket()
	{
		tsDir.beforeFirst();
		tsDir.next();
		int num_directorios = (int) Math.pow(2, pisos_global);

		//El identificador esta entero(con pisos_global caracteres)
		//Buscar bucket a editar(bucket insert)
		RID aux = getBucketaEditar(bucket_insert);
		
		tsDir.moveToRid(aux);
		old_bucket();
		//String idxbucketinsert = idxname + bucket_insert;
		//TableInfo tbinsert = new TableInfo(idxbucketinsert, sch);
		//ts = new TableScan(tbinsert, tx);
		
		//Genera una nuevo bucket-->tabla
		//20 en torno al debug
		new_bucket();
		//String idxnewbucket = idxname + bucketnumbers;
		//TableInfo tbnew = new TableInfo(idxnewbucket, sch);
		//tsnewb = new TableScan(tbnew, tx);

		//recorrer el bucket insert y pegar en la nueva tabla todo lo que al hacer hashkey de el nuevo identificador
		//eliminar del bucket insert todo lo que se agrego a la nueva tabla(misma ejecucion)

		while(ts.next())
		{
			int i1 = ts.getInt("block");
			int i2 = ts.getInt("id");
			Constant val = ts.getVal("dataval");
			String newkeyhash = binhash(val, profundidad_local +1);

			if(!newkeyhash.equals(identificador.substring(identificador.length()-profundidad_local-1))){
				transfer_record(i1, i2, val);
				//tsnewb.insert();
				//tsnewb.setInt("block", i1);
				//tsnewb.setInt("id", i2);
				//tsnewb.setVal("dataval", val);
				//ts.delete();
			}
		}
		
		tsDir.beforeFirst();
		tsDir.next();
		while(tsDir.next())
		{
			int auxbucket = tsDir.getInt(dir2);
			if(auxbucket == bucket_insert)
			{
				String identificador_cortado = identificador.substring(profundidad_local - 1);
				String auxid = tsDir.getString(dir1).substring(profundidad_local - 1);
				tsDir.setInt(dir3, profundidad_local +1);
				if(!auxid.equals(identificador_cortado))
					tsDir.setInt(dir2, bucketnumbers);
			}
		}
		
		
		//num buckets
		bucketnumbers++;
		tsDir.beforeFirst();
		tsDir.next();
		tsDir.setInt(dir3, bucketnumbers);	
		
	}
	
	private void ExtendHashEditDir(int num_directorios)
	{
		//Add lefts zeros
		for(int i = 0; i < num_directorios; i++)
		{
			tsDir.next();
			String prev_identificador = tsDir.getString(dir1);
			tsDir.setString(dir1, "0"+prev_identificador );
		}
		//POR AHORA SERAN INSERTS, puede ser optimizable
		//ADD new records
		for(int i = num_directorios; i < num_directorios * 2; i++)
		{
			int bn = Math.floorDiv(i-num_directorios+1,17);
			int id = (i-num_directorios+1)%17;
			RID aux = new RID(bn,id);
			tsDir.moveToRid(aux);
			int bucket = tsDir.getInt(dir2);
			int profundidad = tsDir.getInt(dir3);
			tsDir.insert();
			tsDir.setString(dir1, Integer.toBinaryString(i));
			tsDir.setInt(dir2, bucket);
			tsDir.setInt(dir3, profundidad);
		}
	}

	private RID getBucketaEditar(int bi)
	{
		int bn = Math.floorDiv(bi+1,17);
		int id = (bi+1)%17;
		return new RID(bn,id);
	}

	private void old_bucket()
	{
		String idxbucketinsert = idxname + bucket_insert;
		TableInfo tbinsert = new TableInfo(idxbucketinsert, sch);
		ts = new TableScan(tbinsert, tx);
	}

	private void new_bucket()
	{
		String idxnewbucket = idxname + bucketnumbers;
		TableInfo tbnew = new TableInfo(idxnewbucket, sch);
		tsnewb = new TableScan(tbnew, tx);
	}

	private void transfer_record(int i1, int i2, Constant val)
	{
		tsnewb.insert();
		tsnewb.setInt("block", i1);
		tsnewb.setInt("id", i2);
		tsnewb.setVal("dataval", val);
		ts.delete();
	}
}
