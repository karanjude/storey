package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private final int gbfield;
	private final Type gbfieldtype;
	private final int afield;
	private final Op what;

	/**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
    }
    
    private Map<Field, Tuple> groupedTuples = new HashMap<Field,Tuple>();
    private Map<Field, Integer> groupedTuplesCount = new HashMap<Field,Integer>();

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
    	Field groupByFiled = tup.getField(gbfield);
    	
    	if(!groupedTuples.containsKey(groupByFiled)){
    		Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE };
    		TupleDesc mergedTupleDesc = new TupleDesc(
    				types
    				);
    		Tuple mergedTuple = new Tuple(mergedTupleDesc);
    		mergedTuple.setField(gbfield, groupByFiled);
    		mergedTuple.setField(afield, new IntField(1));
    		groupedTuples.put(groupByFiled, mergedTuple);
    	}else{
    		Tuple mergedTuple = groupedTuples.get(groupByFiled);
    		mergedTuple.setField(afield, new IntField(mergedTuple.getField(afield).hashCode() + 1));
    	}
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
    				iterator = groupedTuples.entrySet().iterator();
    			}
    			
    			@Override
    			public Tuple next() throws DbException, TransactionAbortedException,
    					NoSuchElementException {
    				current = iterator.next().getValue();
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
