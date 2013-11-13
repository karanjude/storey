package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeSet;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

    private final int gbfield;
	private final Type gbfieldtype;
	private final int afield;
	private final Op what;
	boolean hasRecords = false;

	/**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		if(NO_GROUPING != gbfield)
			groupingStrategy = new GroupingStrategy(gbfield, afield, what, gbfieldtype);
		else
			groupingStrategy = new NoGroupingStrategy(gbfield,afield, what, gbfieldtype);
		this.what = what;
    }
    

	private GroupingStrategy groupingStrategy;
    
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
    	//System.out.println("tup : " + tup);
    	groupingStrategy.merge(tup);
    	
    	hasRecords = true;
    }



	/**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	
    	return new DbIterator() {
			
			private Iterator<Entry<Field, Tuple>> iterator;
			private Tuple current;

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				open();
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				iterator = groupingStrategy.groupedTuplesIterator();
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException,
					NoSuchElementException {
				Entry<Field, Tuple> n = iterator.next();
				current = n.getValue();
				return current;
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				return iterator.hasNext();
			}
			
			@Override
			public TupleDesc getTupleDesc() {
				return current.getTupleDesc();
			}
			
			@Override
			public void close() {
			}
		};
    	
    }

}
