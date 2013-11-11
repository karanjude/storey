package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {

    private final int tableid;
	private final DbIterator child;
	private final TransactionId t;
	boolean opened = false;

	/**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
			this.t = t;
			this.child = child;
			this.tableid = tableid;
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
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	open();
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
    	if(!opened)
    		return null;
    	
    	int recordsInserted = 0;
    	
    	while(child.hasNext()){
    		Tuple tuple = child.next();
    		try {
    			DbFile dbFile = Database.getCatalog().getDbFile(this.tableid);
    			dbFile.addTuple(t, tuple);
    			recordsInserted++;
    		} catch (IOException e) {
    			throw new DbException(" could not insert db record");
    		}
    	}

    	opened = false;
    	
    	Tuple result = new Tuple(getTupleDesc());
    	result.setField(0, new IntField(recordsInserted));
    	
        return result;
    }
}
