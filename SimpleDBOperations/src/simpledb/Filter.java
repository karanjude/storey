package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {

    private final Predicate p;
	private final DbIterator child;

	/**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
		this.p = p;
		this.child = child;
    }

    public TupleDesc getTupleDesc() {
    	return child.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
    	child.open();
    }

    public void close() {
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
    	
    	Tuple tupleToReturn = null;
    	
    	while(child.hasNext()){
    		Tuple tupleToTest = child.next();
    		if(p.filter(tupleToTest)){
    			tupleToReturn = tupleToTest;
    			break;
    		}
    	}
        return tupleToReturn;
    }
}
