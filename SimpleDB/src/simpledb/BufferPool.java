package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private Map<PageId, Page > cachedPages = new HashMap<PageId, Page>();
    private Map<PageId, Date> lastAccessedTimeStamps = new HashMap<PageId, Date>();
    private Map<PageId, PageLock> lockMap = new HashMap<PageId, PageLock>();
    private Map<TransactionId, List<PageLock>> transactionLocks = new HashMap<TransactionId, List<PageLock>>();
    
	private final int numPages;

	private PageId pid;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
		this.numPages = numPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
    	HeapFile heapFile = (HeapFile) dbFile;

    	if(cachedPages.containsKey(pid)){
    		
    		if(null == acquireLock(pid, perm, tid)){
    		}

    		lastAccessedTimeStamps.put(pid, new Date());
    		return cachedPages.get(pid);
    	}
    	
    	HeapPage result = (HeapPage) heapFile.readPage(pid);
    	
    	if(null != result){
    		PageLock newLock = PageLock.newLock(pid, perm, tid);
			
    		if(null != newLock){
    			cachedPages.put(pid, result);
    			lastAccessedTimeStamps.put(pid, new Date());
    			lockMap.put(pid, newLock);
    			if(cachedPages.size() > numPages)
    				evictPage(tid);
    		}
    	}
    	
		return result;    	
    }
    
    private void reloadPage(PageId pid, TransactionId tid) throws DbException{
    	DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
    	HeapFile heapFile = (HeapFile) dbFile;

    	HeapPage result = (HeapPage) heapFile.readPage(pid);
    	
    	if(null != result){
    		cachedPages.put(pid, result);
    		
    		if(cachedPages.size() > numPages)
    			evictPage(tid);
    	}

    }

    private PageLock acquireLock(PageId pid, Permissions perm, TransactionId tid) throws TransactionAbortedException {
    	PageLock pageLock = lockMap.get(pid);
    	if(null != pageLock){
    		return pageLock.acquireLock(perm, tid);
    	}
    	return null;
    }



	/**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
    	PageLock pageLock = lockMap.get(pid);
    	pageLock.releaseLock(tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId p) {
    	PageLock pageLock = lockMap.get(p);
    	
    	return pageLock.hasLock(tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {

    	List<PageLock> locksHeld = transactionLocks.get(tid);
    	if(null == locksHeld)
    		return;
    	
    	for (PageLock pageLock : locksHeld) {
    		pageLock.releaseLock(tid);
    		PageId pageId = pageLock.getPageId();

    		if(commit && isDirty(pageId))
    			flushPage(pageId);
    		else if(!commit && isDirty(pageId)){
    			try {
    				reloadPage(pageId, tid);
    			} catch (DbException e) {
    				e.printStackTrace();
    			}
    		}
    		
    		if(lockMap.containsKey(pageId) && (pageLock.hasNoLocks() || pageLock.hasExclusiveLock(tid))){
    			lockMap.remove(pageId);
    		}
    				
		}
    	
    	transactionLocks.remove(tid);
    	
    	//TODO:: FIX FALLBACK, THIS SHOULD NEVER HAPPEN
    	for (Entry<PageId, PageLock> entry : lockMap.entrySet()) {
			PageLock l = entry.getValue();
			if(l.hasLock(tid)){
				l.releaseLock(tid);
				if(l.hasNoLocks())
					lockMap.remove(entry.getKey());
			}
		}
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	
    	PageId pageId = null;
    	HeapPage pageToUpdate = null;
    	
    	if(null != t.getRecordId())
    		pageId = t.getRecordId().getPageId();
    	
    	if(null != pageId)
    		pageToUpdate = (HeapPage) getPage(tid, pageId, Permissions.READ_WRITE);
    	
    	if(null == pageToUpdate)
    		pageToUpdate = createNewPage(tid, pageId);
    	
    	lastAccessedTimeStamps.put(pageToUpdate.getId(), new Date());
    	
    	pageToUpdate.addTuple(t);
    	pageToUpdate.markDirty(true, tid);
    }

    private HeapPage createNewPage(TransactionId tid, PageId pageId) throws IOException {
    	byte[] heapPageBuffer = new byte[BufferPool.PAGE_SIZE];
    	
    	HeapPageId heapPageId = (HeapPageId) pageId;
    	HeapPage page = new HeapPage(heapPageId, heapPageBuffer);
    	
    	return page;
	}

	/**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	
    	PageId pageId = t.getRecordId().getPageId();
    	HeapPage pageToUpdate = (HeapPage) getPage(tid, pageId, null);
    	
    	lastAccessedTimeStamps.put(pageToUpdate.getId(), new Date());
    	
    	pageToUpdate.deleteTuple(t);
    	pageToUpdate.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (Entry<PageId, Page> pageEntry : cachedPages.entrySet()) {
    			flushPage(pageEntry.getKey());
		}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
    	Page dirtyPage = cachedPages.get(pid);
    	
    	if(null == dirtyPage.isDirty())
    		return;
    		
    	int tableId = dirtyPage.getId().getTableId();
		DbFile dbFile = Database.getCatalog().getDbFile(tableId);
		try {
			dbFile.writePage(dirtyPage);
			dirtyPage.markDirty(false, null);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @param tid 
     */
    private synchronized  void evictPage(TransactionId tid) throws DbException {
    	
    	Page oldestPageToEvict = findOldestPageWhichHasNoLocksAndIsNotDirtyToEvict();
    	
    		try {
				flushPage(oldestPageToEvict.getId());
			} catch (IOException e) {
				e.printStackTrace();
			}
    	
    	cachedPages.remove(oldestPageToEvict.getId());
    	lastAccessedTimeStamps.remove(oldestPageToEvict.getId());
    	
    	//System.out.println("lockmap:size:before eviction "+lockMap.size());
    	
    	PageLock removed =  lockMap.remove(oldestPageToEvict.getId());
    	
    	if(null == removed){
    		//	System.out.println("pagelock is empty");
    		throw new DbException("Cannot evict page not suitable page to evict found ");
    	}
    	
    	if(null != removed){
    		//System.out.println("pageLock removed: " + removed + " has exclusive locks:" + removed.hasExclusiveLock());
    		removed.releaseLock(tid);
    		//System.out.println("pageLock removed: " + removed + " has exclusive locks:" + removed.hasExclusiveLock());
    	}
    	
    	//System.out.println("lockmap:size:after eviction "+lockMap.size());
    }

	private Page findOldestPageWhichHasNoLocksAndIsNotDirtyToEvict() throws DbException {
		PageId oldestPageid = null;
		Date oldestDate = null;
		boolean found = false;
		
		for ( Entry<PageId, Date> entry : lastAccessedTimeStamps.entrySet()) {
			boolean hasExclusiveLock = hasExclusiveLock(entry.getKey());
			boolean isDirty = isDirty(entry.getKey());
			if(hasExclusiveLock && isDirty)
				continue;
			
			if(null == oldestPageid){
				oldestPageid = entry.getKey();
				oldestDate = entry.getValue();
				found = true;
				continue;
			}
			
			if(entry.getValue().before(oldestDate)){
				oldestPageid = entry.getKey();
				oldestDate = entry.getValue();
			}
			
		}
		

		if(!found)
			throw new DbException("no suitable pages found for eviciton");
		
		Page result = cachedPages.get(oldestPageid);
		
		if(null == result)
			System.out.println("!! got empty page !!");
		
		return result;
	}

	private boolean isDirty(PageId key) {
		if(!cachedPages.containsKey(key))
			return false;
		
		return cachedPages.get(key).isDirty() != null;
	}

	private boolean hasExclusiveLock(PageId key) {
		if(!lockMap.containsKey(key))
			return false;
			
		PageLock pageLock = lockMap.get(key);
		return pageLock.hasExclusiveLock();
	}

	public void updateTransactionLock(TransactionId tid, PageLock pageLock) {
		if(!transactionLocks.containsKey(tid)){
			transactionLocks.put(tid, new ArrayList<PageLock>());
		}
		
		List<PageLock> locksHeld = transactionLocks.get(tid);
		//TODO::OPTIMIZE THIS
		
		boolean found = false;
		for (PageLock pl : locksHeld) {
			if(pl.hashCode() == pageLock.hashCode()){
				found = true;
				break;
			}
		}
		
		if(!found){
			locksHeld.add(pageLock);
		}
	}

}
