package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class Locktable {

	class LockId{
		PageId pageId;
		Tuple tuple;
		TransactionId transactionId;
		
		public LockId(PageId pageId, Tuple t, TransactionId tid) {
			this.pageId = pageId;
			tuple = t;
			transactionId = tid;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result
					+ ((pageId == null) ? 0 : pageId.hashCode());
			result = prime * result
					+ ((transactionId == null) ? 0 : transactionId.hashCode());
			result = prime * result + ((tuple == null) ? 0 : tuple.hashCode());
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
			if (!(obj instanceof LockId)) {
				return false;
			}
			LockId other = (LockId) obj;
			if (!getOuterType().equals(other.getOuterType())) {
				return false;
			}
			if (pageId == null) {
				if (other.pageId != null) {
					return false;
				}
			} else if (!pageId.equals(other.pageId)) {
				return false;
			}
			if (transactionId == null) {
				if (other.transactionId != null) {
					return false;
				}
			} else if (!transactionId.equals(other.transactionId)) {
				return false;
			}
			if (tuple == null) {
				if (other.tuple != null) {
					return false;
				}
			} else if (!tuple.equals(other.tuple)) {
				return false;
			}
			return true;
		}
		private Locktable getOuterType() {
			return Locktable.this;
		}
		
	}
	
	Map<Tuple, Integer> tupleRef = new HashMap<Tuple, Integer>();
	Map<PageId, Integer> pageRef = new HashMap<PageId, Integer>();
	Map<LockId, Semaphore> table = new HashMap<Locktable.LockId, Semaphore>();
	Map<TransactionId, Map<LockId, Integer>> tidRef = new HashMap<TransactionId, Map<Locktable.LockId,Integer>>();
	
	public boolean isLocked(Tuple tuple) {
		return tupleRef.containsKey(tuple);
	}

	public synchronized void getLock(PageId pageId, Tuple t, TransactionId tid) {
		LockId lockId = new LockId(pageId, t, tid);
		Semaphore lock = new Semaphore(1);
		
		if(table.containsKey(lockId))
			lock = table.get(lockId);
		else
			table.put(lockId, lock);
		
		try {
			lock.acquire();
			increaseTupleRef(t);
			increasePageRef(pageId);
			addTidRef(tid, lockId);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	public synchronized void releaseLock(PageId pageId, Tuple t, TransactionId tid) {
		LockId lockId = new LockId(pageId, t, tid);
		if(table.containsKey(lockId)){
			Semaphore lock = table.get(lockId);
			lock.release();
			table.remove(lockId);
			decreaseTupleRef(t);
			decreasePageRef(pageId);
			removeTidRef(tid, lockId);
		}
	}
	
	private void addTidRef(TransactionId tid, LockId lockId) {
		if(!tidRef.containsKey(tid))
			tidRef.put(tid, new HashMap<Locktable.LockId, Integer>());
		
		Map<LockId, Integer> lockidsForTid = tidRef.get(tid);
		lockidsForTid.put(lockId, 1);
	}

	private void increasePageRef(PageId pageId) {
		if(!pageRef.containsKey(pageId))
			pageRef.put(pageId, 0);
		
		Integer pageRefCount = pageRef.get(pageId);
		pageRef.put(pageId, pageRefCount + 1);
	}

	private void increaseTupleRef(Tuple t) {
		if(!tupleRef.containsKey(t))
			tupleRef.put(t, 0);
		
		try{
			Integer tupleRefCount = tupleRef.get(t);
			tupleRef.put(t, tupleRefCount + 1);
		}catch(NullPointerException e){
			System.out.println("");
		}
	}


	private void removeTidRef(TransactionId tid, LockId lockId) {
	}

	private void decreasePageRef(PageId pageId) {
		if(!pageRef.containsKey(pageId))
			return;
		try{
			Integer pageRefCount = pageRef.get(pageId);
			pageRefCount = pageRefCount - 1;
			pageRef.put(pageId, pageRefCount);
			
			if(pageRefCount <= 0)
				pageRef.remove(pageId);
		}catch(NullPointerException e){
			System.out.println("");
		}
	}

	private void decreaseTupleRef(Tuple t) {
		if(!tupleRef.containsKey(t))
			return;
			
		Integer refCount = tupleRef.get(t);
		refCount = refCount - 1;
		tupleRef.put(t, refCount);
		
		if(refCount <= 0)
			tupleRef.remove(t);
	}

	public boolean hasExclusivelock(PageId pageId) {
		if(!pageRef.containsKey(pageId))
			return false;
		
		Integer pageRefCount = pageRef.get(pageId);
		return pageRefCount != 0;
	}

	public synchronized List<PageId> releaseLock(TransactionId tid) {
		List<PageId> lockedPages = new ArrayList<PageId>();
		
		if(!tidRef.containsKey(tid))
			return lockedPages;
		
		for (LockId lid : tidRef.get(tid).keySet()) {
			if(!lockedPages.contains(lid.pageId))
				lockedPages.add(lid.pageId);
			
			if(table.containsKey(lid)){
				Semaphore lock = table.get(lid);
				lock.release();
				table.remove(lid);
			}
			
			decreaseTupleRef(lid.tuple);
			decreasePageRef(lid.pageId);
		}
		
		tidRef.remove(tid);

		return lockedPages;
	}

	public void reset() {
		table.clear();
		tidRef.clear();
		pageRef.clear();
		tupleRef.clear();
	}

}
