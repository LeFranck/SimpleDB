package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import simpledb.file.Block;
import simpledb.buffer.Buffer;
import simpledb.server.SimpleDB;
import java.util.*;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */
public class RecoveryMgr {
   private int txnum;

   /**
    * Creates a recovery manager for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RecoveryMgr(int txnum) {
      this.txnum = txnum;
      new StartRecord(txnum).writeToLog();
   }

   /**
    * Writes a commit record to the log, and flushes it to disk.
    * Cambio a undo-redo, asi que flushea el log con los respectivos updates de esta transaccion y 
    * los commits que quedaron fuera de otras.
    * Luego escribe a disco los cambios hechos por esta transacción
    * finalmente escribe en el log buffer commit Txid 
    */
   public void commit() {
	  int lsn = SimpleDB.logMgr().currentLSN();
	  System.out.println("--Flush-Log-by-"+txnum+"--");
      SimpleDB.logMgr().flush(lsn);
	  System.out.println("--Done-Flush-Log-by-"+txnum+"--");
      SimpleDB.bufferMgr().flushAll(txnum);
      new CommitRecord(txnum).writeToLog();
   }

   /**
    * Writes a rollback record to the log, and flushes it to disk.
    */
   public void rollback() {
      doRollback();
      int lsn = new RollbackRecord(txnum).writeToLog();
	  System.out.println("--Flush-Log-by-"+txnum+"--");
      SimpleDB.logMgr().flush(lsn);
	  System.out.println("--Done-Flush-Log-by-"+txnum+"--");
      SimpleDB.bufferMgr().flushAll(txnum);
   }

   /**
    * Recovers uncompleted transactions from the log,
    * then writes a quiescent checkpoint record to the log and flushes it.
    */
   public void recover() {
      doRecover();
      int lsn = new CheckpointRecord().writeToLog();
	  System.out.println("--Flush-Log-by-"+txnum+"--");
      SimpleDB.logMgr().flush(lsn);
	  System.out.println("--Done-Flush-Log-by-"+txnum+"--");
      SimpleDB.bufferMgr().flushAll(txnum);
   }

   /**
    * Writes a setint record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.getInt(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetIntRecord(txnum, blk, offset, oldval, newval).writeToLog();
   }

   /**
    * Writes a setstring record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.getString(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetStringRecord(txnum, blk, offset, oldval, newval).writeToLog();
   }

   /**
    * Rolls back the transaction.
    * The method iterates through the log records,
    * calling undo() for each log record it finds
    * for the transaction,
    * until it finds the transaction's START record.
    */
   private void doRollback() {
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(txnum);
         }
      }
   }

   /**
    * Does a complete database recovery.
    * The method iterates through the log records.
    * Whenever it finds a log record for an unfinished
    * transaction, it calls undo() on that record.
    * The method stops when it encounters a CHECKPOINT record
    * or the end of the log.
    * When stop looking, then go over the logs records again, 
    * in chronological order, redoing all the work of committed transactions
    */
   private void doRecover() {
      Collection<Integer> finishedTxs = new ArrayList<Integer>();
      ArrayList<Integer> CommitedTxs = new ArrayList<Integer>();
      ArrayList<LogRecord> LogRecords = new ArrayList<LogRecord>();
      
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         LogRecords.add(rec);
         if (rec.op() == CHECKPOINT)
            return;
         if (rec.op() == COMMIT || rec.op() == ROLLBACK)
            finishedTxs.add(rec.txNumber());
         if(rec.op() == COMMIT)
        	 CommitedTxs.add(rec.txNumber());
         else if (!finishedTxs.contains(rec.txNumber()))
            rec.undo(txnum);
      }
      for(int i = LogRecords.size()-1 ;i>-1 ;i--)
      {
    	  LogRecord rec = LogRecords.get(i);    	  
    	  System.out.println(rec.toString());
      }      
      
      
      for(int i = LogRecords.size()-1 ;i>-1 ;i--)
      {
    	  LogRecord rec = LogRecords.get(i);
    	  if(tx_was_commited(rec.txNumber(), CommitedTxs))
    		  rec.redo(txnum);
      }
      
   }
   
   /**
    * It returns true if the transaction was committed
    * @param txnumber transaction to ask for
    * @param CommitedTxs arraylist whit the commited transactions 
    */
   private boolean tx_was_commited(int txnumber, ArrayList<Integer> CommitedTxs)
   {
	   for(int i = 0; i < CommitedTxs.size(); i++)
		   if(txnumber == CommitedTxs.get(i))
			   return true;
	   return false;
   }

   /**
    * Determines whether a block comes from a temporary file or not.
    */
   private boolean isTempBlock(Block blk) {
      return blk.fileName().startsWith("temp");
   }
}
