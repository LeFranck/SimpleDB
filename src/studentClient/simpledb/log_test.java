package studentClient.simpledb;

import java.util.Iterator;

import simpledb.log.BasicLogRecord;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

public class log_test {

	public static void main(String[] args) {

		SimpleDB.initFileAndLogMgr("testdb0");
		LogMgr logmgr = SimpleDB.logMgr();
		int lsn1 = logmgr.append(new Object[]{"a","b"});
		int lsn2 = logmgr.append(new Object[]{"c","d"});
		int lsn3 = logmgr.append(new Object[]{"e","f"});
		logmgr.flush(lsn3);

		Iterator<BasicLogRecord> iter = logmgr.iterator();

		while( iter.hasNext())
		{
			BasicLogRecord rec = iter.next();
			String v1 = rec.nextString();
			String v2 = rec.nextString();
			System.out.println("[" + v1 +","+ v2 +"]");
		}
		
	}

}
