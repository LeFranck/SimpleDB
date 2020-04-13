package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.file.Block;
import simpledb.index.Index;
import simpledb.index.btree.DirEntry;

public class HashDir{

   private TableInfo ti;
   private Transaction tx;
   private String filename;
   private HashPage contents;

   public HashDir(Block blk, TableInfo ti, Transaction tx) {

   }



}