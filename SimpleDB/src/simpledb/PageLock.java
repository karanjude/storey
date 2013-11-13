package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class PageLock {

	private  int sharedLockCount;
	private  int exclusiveLockCount;
	private List<TransactionId> listOfTransactionsHoldingSharedLock = new ArrayList<TransactionId>();
	private List<TransactionId> listOfTransactionsHoldingExclusiveLock = new ArrayList<TransactionId>();
	private final PageId pid;
	private Semaphore exclusiveLock = new Semaphore(1);
	private CountDownLatch lock = new CountDownLatch(1);
	private Map<TransactionId, Integer> trialMap = new HashMap<TransactionId, Integer>();
	
	
	private PageLock(int sharedLockCount, int exclusiveLockCount, TransactionId tid, PageId pid) {
		this.sharedLockCount = sharedLockCount;
		this.exclusiveLockCount = exclusiveLockCount;
		this.pid = pid;
	}

	public static PageLock newLock(PageId pid, Permissions perm, TransactionId tid) throws TransactionAbortedException {
		int sharedLockCount = 0;
		int exclusiveLockCount = 0;
		
		PageLock pageLock = new PageLock(sharedLockCount, exclusiveLockCount, tid, pid);
		return pageLock.acquireLock(perm, tid);
	}

	public boolean hasExclusiveLock() {
		return exclusiveLockCount > 0;
	}
	
	private boolean retryGettingSharedLock(TransactionId tid)
			throws InterruptedException {
		boolean acquired = lock.await(200, TimeUnit.MILLISECONDS);
		if(!acquired){
			if(!trialMap.containsKey(tid)){
				trialMap.put(tid, 1);
			}
			int trial = trialMap.get(tid);
			if(trial > 3){
				trialMap.remove(tid);
				return false;
			}else{
				Thread.currentThread().sleep(1000 * trial);
				trial += 1;
				trialMap.put(tid, trial);
				retryGettingLock(tid);
			}
		}
		return true;
	}


	private void acquireSharedLock(TransactionId tid) {
		if(hasExclusiveLock() && !listOfTransactionsHoldingExclusiveLock.contains(tid)){
			try {
				retryGettingSharedLock(tid);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		if(!listOfTransactionsHoldingSharedLock.contains(tid)){
			sharedLockCount++;
			listOfTransactionsHoldingSharedLock.add(tid);
		}
	}

	private void acquireExclusiveLock(TransactionId tid) throws TransactionAbortedException {
		
		
		if(!listOfTransactionsHoldingExclusiveLock.contains(tid)){
		try {
			
			boolean acquired = retryGettingLock(tid);
			exclusiveLockCount++;
			listOfTransactionsHoldingExclusiveLock.add(tid);
			
			if(!acquired){
				boolean released = tryAskingTrasactionHoldingTheLockToRelinguish(tid);
				if(released)
					acquired = retryGettingLock(tid);
			}
			if(!acquired)
				throw new TransactionAbortedException("Transaction was aborted " + tid + " was aborted as it could not acquire lock on page : " + pid);
			
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
			
//		try {
//			exclusiveLock.acquire();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		}
	}

	private boolean tryAskingTrasactionHoldingTheLockToRelinguish(
			TransactionId tid) {
		boolean isDirty = Database.getBufferPool().isDirty(pid);
		if(!isDirty){
			for (TransactionId t : listOfTransactionsHoldingExclusiveLock) {
				this.releaseExclusiveLock(t);
			}
			return true;
		}
		return false;
	}

	private boolean retryGettingLock(TransactionId tid)
			throws InterruptedException {
		boolean acquired = exclusiveLock.tryAcquire(200, TimeUnit.MILLISECONDS);
		if(!acquired){
			if(!trialMap.containsKey(tid)){
				trialMap.put(tid, 1);
			}
			int trial = trialMap.get(tid);
			if(trial > 0){
				trialMap.remove(tid);
				return false;
			}else{
				Thread.currentThread().sleep(1000 * trial);
				trial += 1;
				trialMap.put(tid, trial);
				retryGettingLock(tid);
			}
		}
		return true;
	}

	public void releaseLock(TransactionId tid) {
		if(listOfTransactionsHoldingSharedLock.contains(tid))
			releaseSharedLock(tid);
		else if(listOfTransactionsHoldingExclusiveLock.contains(tid))
			releaseExclusiveLock(tid);
	}

	private void releaseExclusiveLock(TransactionId tid) {
		listOfTransactionsHoldingExclusiveLock.remove(tid);
		exclusiveLockCount--;
		exclusiveLock.release();
		lock.countDown();
	}

	private void releaseSharedLock(TransactionId tid) {
		listOfTransactionsHoldingSharedLock.remove(tid);
		sharedLockCount--;
		
	}

	public boolean hasLock(TransactionId tid) {
		if(listOfTransactionsHoldingSharedLock.contains(tid))
			return true;
		else if(listOfTransactionsHoldingExclusiveLock.contains(tid))
			return true;
		
		return false;
	}
	
	public PageLock acquireLock(Permissions perm, TransactionId tid) throws TransactionAbortedException {
		
    	if(perm == Permissions.READ_ONLY && listOfTransactionsHoldingSharedLock.contains(tid))
    		return this;
    	else if(perm == Permissions.READ_WRITE && listOfTransactionsHoldingExclusiveLock.contains(tid))
    		return this;
    	
    	boolean hasExclusiveLock = this.hasExclusiveLock();
    	
    	if(perm == Permissions.READ_ONLY && !hasExclusiveLock){
    		this.acquireSharedLock(tid);
    	}else if(perm == Permissions.READ_ONLY && hasExclusiveLock){
    		this.acquireSharedLock(tid);
    	}else if(perm == Permissions.READ_WRITE && !hasExclusiveLock){
    		this.acquireExclusiveLock(tid);
    	}else if(perm == Permissions.READ_WRITE && hasExclusiveLock ){
    		this.acquireExclusiveLock(tid);
    	}

		return this;
	}

	public PageId getPageId() {
		return pid;
	}

	public boolean hasNoLocks() {
		return 0 == sharedLockCount && 0 == exclusiveLockCount;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pid == null) ? 0 : pid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof PageLock)) {
			return false;
		}
		PageLock other = (PageLock) obj;
		if (pid == null) {
			if (other.pid != null) {
				return false;
			}
		} else if (!pid.equals(other.pid)) {
			return false;
		}
		return true;
	}

	public boolean hasExclusiveLock(TransactionId tid) {
		if(listOfTransactionsHoldingExclusiveLock.contains(tid))
			return true;
		
		return false;
	}


}
