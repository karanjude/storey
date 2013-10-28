package simpledb;

import java.awt.HeadlessException;
import java.io.*;
import java.util.*;

import javax.naming.BinaryRefAddr;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
	private final TupleDesc td;
	private int numPages;
	private Map<PageId, HeapPage> cachedPages = new HashMap<PageId, HeapPage>();
	private byte[] buffer;

	/**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
		this.f = f;
		this.td = td;
		this.numPages = 1;
		//readAllPages();
    }


	/**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
    	return this.f;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
    	return this.f.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return this.td;
    }
    
    private void readAllPages() {
    	byte[] buffer = new byte[BufferPool.PAGE_SIZE];
    	
       	try {
			FileInputStream reader = new FileInputStream(this.f);
			int read = 0;
			int pageCount = 0;
			while((read = reader.read(buffer)) != -1){
				cacheHeapPage(pageCount, buffer);
				pageCount++;
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


    private void cacheHeapPage(int pageCount, byte[] buffer) {
    	HeapPageId pageId = new HeapPageId(getId(), pageCount);
    	HeapPage result = null;
		
    	if(cachedPages.containsKey(pageId))
    		return;
    	
		try {
			result = new HeapPage(pageId, buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		cachedPages.put(pageId, result);
		
	}

    
	protected HeapPage readPage(HeapPageId heapPageId, byte[] buffer) {
		this.buffer = buffer;
		return (HeapPage) readPage(heapPageId);
	}


	// see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	RandomAccessFile randomAccessFile = null;
    	
    	try {
			randomAccessFile = new RandomAccessFile(getFile(), "r" );
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			return null;
		}
    	
    	byte[] heapPageBuffer = new byte[BufferPool.PAGE_SIZE];
    	
    	try {
    		randomAccessFile.skipBytes(pid.pageno() * BufferPool.PAGE_SIZE);
			randomAccessFile.read(heapPageBuffer, 0,  BufferPool.PAGE_SIZE);
		} catch (IOException e1) {
			e1.printStackTrace();
			heapPageBuffer = null;
			return null;
		} catch(IndexOutOfBoundsException e){
			e.printStackTrace();
			heapPageBuffer = null;
		}
    	
		HeapPageId p = (HeapPageId) pid;
    	HeapPage result = null;
    	
    	if(null != heapPageBuffer){
    		try {
    			result = new HeapPage(p, heapPageBuffer);
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
		
    	try {
			randomAccessFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

    	return result;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	
    	PageId pageId = t.getRecordId().getPageId();
    	HeapPageId heapPageId = new HeapPageId(this.getId(), pageId.pageno());
    	RecordId rid = new RecordId(heapPageId, t.getRecordId().tupleno());
    	
		t.setRecordId(rid);
    	Page page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, null);
    	Database.getBufferPool().insertTuple(tid, heapPageId.getTableId(), t);

    	ArrayList<Page> pagesModified = new ArrayList<Page>();
    	pagesModified.add(page);
    	
        return pagesModified;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	
       	PageId pageId = t.getRecordId().getPageId();
    	Page page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, null);
    	Database.getBufferPool().deleteTuple(tid, t);
    	
        return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	final TransactionId transactionId = tid;
    	final HeapFile heapFile = this;
    	
    	return new DbFileIterator() {
			boolean isOpened = false;
			FileInputStream inputStream = null;
			HeapPage page = null;
			int pageNo = 0;
			HeapPage heapPage = null;
			byte[] buffer = new byte[BufferPool.PAGE_SIZE];
			Iterator<Tuple> tupleIterator = null;
			
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				close();
				open();
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				isOpened = true;
				try {
					inputStream = new FileInputStream(heapFile.f);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				pageNo = 0;
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException,
					NoSuchElementException {
				
				if(null != tupleIterator)
					return tupleIterator.next();
				
				throw new NoSuchElementException();
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if(!isOpened)
					return false;

				if(!currentHeapPageHasMoreTuples()){
					heapPage = loadNextPage();
					if(null != heapPage){
						tupleIterator = heapPage.iterator();
					}
				}
				
				/* no more heap pages, we just crossed the last heap page */
				if(null == heapPage){
					tupleIterator = null;
					return false;
				}
				
				if(null == tupleIterator && null != heapPage){
					tupleIterator = heapPage.iterator();
				}

				return tupleIterator.hasNext();
			}
			
			private boolean currentHeapPageHasMoreTuples() {
				if(null == heapPage)
					return false;
				
				return tupleIterator.hasNext();
			}

			private HeapPage loadNextPage() {
				int read = -1;
				try {
					read = inputStream.read(buffer);
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
				
				if(-1 == read || BufferPool.PAGE_SIZE != read)
					return null;
				
				HeapPageId heapPageId = new HeapPageId(heapFile.getId(), pageNo);
				
				try {
					heapPage =  (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, null);
					pageNo++;
				} catch (TransactionAbortedException e) {
					e.printStackTrace();
					heapPage = null;
				} catch (DbException e) {
					e.printStackTrace();
					heapPage = null;
				}
				
//				if(cachedPages.containsKey(heapPageId))
//					return cachedPages.get(heapPageId);
//				
//				heapPage = (HeapPage) heapFile.readPage(heapPageId, buffer);
				
//				try {
//					heapPage = new HeapPage(heapPageId, buffer);
//				} catch (IOException e) {
//					e.printStackTrace();
//					return null;
//				}
				
				return heapPage;
			}

			@Override
			public void close() {
				isOpened = false;
				heapPage = null;
				tupleIterator = null;
				
				try {
						inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
    	
    }


    
}

