package simpledb;
import java.awt.HeadlessException;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private final TransactionId tid;
	private final int tableid;
	private final String tableAlias;

	/**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
		this.tid = tid;
		this.tableid = tableid;
		this.tableAlias = tableAlias;
    }

    DbFile heapFile;
    DbFileIterator iterator;

    public void open()
        throws DbException, TransactionAbortedException {
    	if(null == heapFile){
    		heapFile = Database.getCatalog().getDbFile(tableid);
    	}
    	
		iterator = heapFile.iterator(tid);
		
		if(null != iterator)
			iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
       	if(null == heapFile){
    		heapFile = Database.getCatalog().getDbFile(tableid);
    	}
    	
    	return heapFile.getTupleDesc();
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
    	if(null == iterator)
    		return false;
    	
    	return iterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
    	if(null == iterator){
    		throw new NoSuchElementException("iterator null");
    	}
    	
    	Tuple n = iterator.next();
    	
    	//System.out.println(n);
    	
    	return n;
    }

    public void close() {
    	if(null != iterator)
    		iterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
    	iterator.rewind();
    }
}
