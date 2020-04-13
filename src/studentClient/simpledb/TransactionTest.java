package studentClient.simpledb;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;



public class TransactionTest {

	public static void main(String[] args) {
		SimpleDB.init("testdb0");
		TestA t1 = new TestA(); new Thread(t1).start();
		TestB t2 = new TestB(); new Thread(t2).start();
		TestC t3 = new TestC(); new Thread(t3).start();
	}

}

class TestA implements Runnable
{
	public void run()
	{
		try
		{
			Transaction tx = new Transaction();
			Block blk1 = new Block("junk", 1);
			Block blk2 = new Block("junk", 2);
			tx.pin(blk1);
			tx.pin(blk2);
			System.out.println("TX A: read 1 start");
			tx.getInt(blk1, 0);
			System.out.println("TX A: read 1 end");
			Thread.sleep(1000);
			System.out.println("Tx A: read 2 Start");
			tx.getInt(blk2, 0);
			System.out.println("Tx A: read 2 end");
			tx.commit();
		}
		catch(InterruptedException e){};
	}
}


class TestB implements Runnable
{
	public void run()
	{
		try
		{
			Transaction tx = new Transaction();
			Block blk1 = new Block("junk", 1);
			Block blk2 = new Block("junk", 2);
			tx.pin(blk1);
			tx.pin(blk2);
			System.out.println("TX B: write 2 start");
			tx.setInt(blk2, 0, 0);
			System.out.println("TX B: write 2 end");
			Thread.sleep(1000);
			System.out.println("Tx B: read 1 Start");
			tx.getInt(blk1, 0);
			System.out.println("Tx B: read 1 end");
			tx.commit();
		}
		catch(InterruptedException e){};
	}
}

class TestC implements Runnable
{
	public void run()
	{
		try
		{
			Transaction tx = new Transaction();
			Block blk1 = new Block("junk", 1);
			Block blk2 = new Block("junk", 2);
			tx.pin(blk1);
			tx.pin(blk2);
			System.out.println("TX B: write 1 start");
			tx.setInt(blk1, 0, 0);
			System.out.println("TX B: write 1 end");
			Thread.sleep(1000);
			System.out.println("Tx B: read 2 Start");
			tx.getInt(blk2, 0);
			System.out.println("Tx B: read 2 end");
			tx.commit();
		}
		catch(InterruptedException e){};
	}
}


