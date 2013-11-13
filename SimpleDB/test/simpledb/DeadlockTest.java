package simpledb;

import simpledb.TestUtil.LockGrabber;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class DeadlockTest extends TestUtil.CreateHeapFile {
  private PageId p0, p1, p2;
  private TransactionId tid1, tid2;
  private Random rand;

  private static final int POLL_INTERVAL = 100;
  private static final int WAIT_INTERVAL = 200;

  // just so we have a pointer shorter than Database.getBufferPool
  private BufferPool bp;

  /**
   * Set up initial resources for each unit test.
   */
  @Before public void setUp() throws Exception {
    super.setUp();

    // clear all state from the buffer pool
    bp = Database.resetBufferPool(BufferPool.DEFAULT_PAGES);

    // create a new empty HeapFile and populate it with three pages.
    // we should be able to add 512 tuples on an empty page.
    TransactionId tid = new TransactionId();
    for (int i = 0; i < 1025; ++i) {
      empty.addTuple(tid, Utility.getHeapTuple(i, 2));
    }

    // if this fails, complain to the TA
    assertEquals(3, empty.numPages());

    this.p0 = new HeapPageId(empty.getId(), 0);
    this.p1 = new HeapPageId(empty.getId(), 1);
    this.p2 = new HeapPageId(empty.getId(), 2);
    this.tid1 = new TransactionId();
    this.tid2 = new TransactionId();
    this.rand = new Random();

    // forget about locks associated to tid, so they don't conflict with
    // test cases
    bp.getPage(tid, p0, Permissions.READ_WRITE).markDirty(true, tid);
    bp.getPage(tid, p1, Permissions.READ_WRITE).markDirty(true, tid);
    bp.getPage(tid, p2, Permissions.READ_WRITE).markDirty(true, tid);
    bp.flushAllPages();
    bp = Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
  }

  /**
   * Helper method to clean up the syntax of starting a LockGrabber thread.
   * The parameters pass through to the LockGrabber constructor.
   */
  public TestUtil.LockGrabber startGrabber(TransactionId tid, PageId pid,
      Permissions perm) {

    LockGrabber lg = new LockGrabber(tid, pid, perm);
    lg.start();
    return lg;
  }

  /**
   * Not-so-unit test to construct a deadlock situation.
   * t1 acquires p0.read; t2 acquires p1.read; t1 attempts p1.write; t2
   * attempts p0.write. Rinse and repeat.
   */
   @Test public void testReadWriteDeadlock() throws Exception {
    System.out.println("testReadWriteDeadlock constructing deadlock:");

    CountDownLatch latch1 = new CountDownLatch(2);
    LockGrabber lg1Read = startGrabber(tid1, p0, Permissions.READ_ONLY, latch1);
    LockGrabber lg2Read = startGrabber(tid2, p1, Permissions.READ_ONLY, latch1);

    latch1.await();
    
    assertTrue(lg1Read.acquired());
    assertTrue(lg2Read.acquired());
    
    // allow read locks to acquire
    Thread.sleep(POLL_INTERVAL);

    CountDownLatch latch2 = new CountDownLatch(2);
    LockGrabber lg1Write = startGrabber(tid1, p1, Permissions.READ_WRITE, latch2);
    LockGrabber lg2Write = startGrabber(tid2, p0, Permissions.READ_WRITE, latch2);
    latch2.await();

    assertTrue(lg1Write.acquired());
    assertTrue(lg2Write.acquired());
    
    System.out.println("testReadWriteDeadlock resolved deadlock");
  }


/**
   * Not-so-unit test to construct a deadlock situation.
   * t1 acquires p0.write; t2 acquires p1.write; t1 attempts p1.write; t2
   * attempts p0.write.
   */
  @Test public void testWriteWriteDeadlock() throws Exception {
    System.out.println("testWriteWriteDeadlock constructing deadlock:");

    CountDownLatch latch1 = new CountDownLatch(2);
    
    LockGrabber lg1Write0 = startGrabber(tid1, p0, Permissions.READ_WRITE, latch1);
    LockGrabber lg2Write1 = startGrabber(tid2, p1, Permissions.READ_WRITE, latch1);
    
    latch1.await();

   assertTrue(lg2Write1.acquired());
  assertTrue(lg1Write0.acquired());
    
    // allow initial write locks to acquire
    Thread.sleep(POLL_INTERVAL);

    CountDownLatch latch2 = new CountDownLatch(2);

    LockGrabber lg1Write1 = startGrabber(tid1, p1, Permissions.READ_WRITE, latch2);
    LockGrabber lg2Write0 = startGrabber(tid2, p0, Permissions.READ_WRITE, latch2);
    
    latch2.await();
    
  assertTrue(lg1Write1.acquired());
  assertTrue(lg2Write0.acquired());
  

    System.out.println("testWriteWriteDeadlock resolved deadlock");
  }

  private LockGrabber startGrabber(TransactionId tid, PageId pid,
		Permissions perm, CountDownLatch latch) {
	  
	  LockGrabber lg = new LockGrabber(tid, pid, perm, latch);
	    lg.start();
	    return lg;
  }

/**
   * Not-so-unit test to construct a deadlock situation.
   * t1 acquires p0.read; t2 acquires p0.read; t1 attempts to upgrade to
   * p0.write; t2 attempts to upgrade to p0.write
   */
  @Test public void testUpgradeWriteDeadlock() throws Exception {
    System.out.println("testUpgradeWriteDeadlock constructing deadlock:");

    CountDownLatch latch1 = new CountDownLatch(2);
    
    LockGrabber lg1Read = startGrabber(tid1, p0, Permissions.READ_ONLY, latch1);
    LockGrabber lg2Read = startGrabber(tid2, p0, Permissions.READ_ONLY, latch1);
    
    latch1.await();
    
    assertTrue(lg1Read.acquired() && lg2Read.acquired());

    // allow read locks to acquire
    Thread.sleep(POLL_INTERVAL);
    
    CountDownLatch latch2 = new CountDownLatch(2);

    LockGrabber lg1Write = startGrabber(tid1, p0, Permissions.READ_WRITE, latch2);
    LockGrabber lg2Write = startGrabber(tid2, p0, Permissions.READ_WRITE, latch2);

    latch2.await();
    
    assertTrue(lg1Write.acquired() && lg2Write.acquired());

    System.out.println("testUpgradeWriteDeadlock resolved deadlock");
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(DeadlockTest.class);
  }

}

