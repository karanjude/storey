package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    private boolean opened;
	private final DbIterator child;
	private TransactionId tid;
	private List<Page> dirtyPages = new ArrayList<Page>();

	/**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
		this.child = child;
		this.tid = t;
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
    	Type[] types = new Type[]{
        		Type.INT_TYPE
        	};
        	
        	TupleDesc tupleDesc = new TupleDesc(types);
        	return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
    	opened = true;
    	child.open();
    }

    public void close() {
    	opened = false;
    	child.close();
    	
    	for (Page dirtyPage : dirtyPages) {
    		int tableId = dirtyPage.getId().getTableId();
    		DbFile dbFile = Database.getCatalog().getDbFile(tableId);
    		try {
				dbFile.writePage(dirtyPage);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
    	if(!opened)
    		return null;
    	
    	int recordsDeleted = 0;
    	
    	Map<PageId, Page> dirtyPagesMap = new HashMap<PageId, Page>();
    	
    	while(child.hasNext()){
    		Tuple tuple = child.next();
    			int tableId = tuple.getRecordId().getPageId().getTableId();
    			DbFile dbFile = Database.getCatalog().getDbFile(tableId);
    			Page page = dbFile.deleteTuple(tid, tuple);
    			if(!dirtyPagesMap.containsKey(page.getId()))
    				dirtyPagesMap.put(page.getId(), page);
    			
    			recordsDeleted++;
    			
    			//System.out.println("Tuple deleted: " + tuple);
    	}

    	opened = false;
    	
    	Tuple result = new Tuple(getTupleDesc());
    	result.setField(0, new IntField(recordsDeleted));
    	
    	dirtyPages.addAll(dirtyPagesMap.values());
    	
        return result;
    }
}
