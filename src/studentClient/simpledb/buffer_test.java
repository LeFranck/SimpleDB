package studentClient.simpledb;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecord;

public class buffer_test {

	public static void main(String[] args) {
	      for(int i = 10-1 ;i>-1 ;i--)
	      {
	    	  System.out.println(i);
	      }
//		SimpleDB.initFileLogAndBufferMgr("testdb0");
//		BufferMgr bm = SimpleDB.bufferMgr();
//
//		Block blk = new Block("student.tbl",0);
//		Buffer buff = bm.pin(blk);
//		//¿46?
//		String sname = buff.getString(46);
//		int gradyr = buff.getInt(38);
//		System.out.println(sname + " gradyear" + gradyr);
	}

}
