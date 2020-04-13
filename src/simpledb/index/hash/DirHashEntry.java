package simpledb.index.hash;

import simpledb.query.Constant;

/**
 * A directory entry has two components: the number of the child block,
 * and the dataval of the first record in that block.
 * @author Edward Sciore
 */
public class DirHashEntry {
   private String dir;
   //era blocknumber
   private int bucketNumber;
   private int local_depth;
   private Object dataval;
   
   /**
    * Creates a new entry for the specified dataval and block number.
    * @param dataval the dataval
    * @param bucketNumber the block number
 * @return 
    */
   public void DirEntry(String dir, int bucketNumber, int local_depth) {
      this.dataval  = dataval;
      this.bucketNumber = bucketNumber;
   }
   
   /**
    * Returns the dataval component of the entry
    * @return the dataval component of the entry
    */
   public Constant dataVal() {
      return (Constant) dataval;
   }
   
   /**
    * Returns the block number component of the entry
    * @return the block number component of the entry
    */
   public int blockNumber() {
      return bucketNumber;
   }
}
