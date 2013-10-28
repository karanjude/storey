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
    		Tuple mergedTuple = new Tuple(tup.getTupleDesc());
    		mergedTuple.setField(gbfield, groupByFiled);
    		mergedTuple.setField(afield, new IntField(tup.getField(afield).hashCode()));
    		groupedTuples.put(groupByFiled, mergedTuple);
    	}else{
    		updateGroupedTuples(groupByFiled, tup);
    	}
    	hasRecords = true;
    }

    private void updateGroupedTuples(Field groupByFiled, Tuple tup) {
    	Tuple groupedTuple = groupedTuples.get(groupByFiled);
    	Field aggregateField = groupedTuple.getField(afield);
    	
    	switch(what){
    	case SUM:
    		int sum = tup.getField(afield).hashCode() + groupedTuple.getField(afield).hashCode();
    		groupedTuple.setField(afield, new IntField(sum));
    		break;
    	case AVG:
    		if(!groupedTuplesCount.containsKey(groupByFiled)){
    			groupedTuplesCount.put(groupByFiled, 1);
    		}

    		int sum1 = tup.getField(afield).hashCode() + aggregateField.hashCode();
    		int cnt = groupedTuplesCount.get(groupByFiled) + 1;
    		
    		groupedTuple.setField(afield, new IntField((int)(sum1 / cnt)));
    		break;
    	case MIN:
    		Predicate p = new Predicate(afield, Predicate.Op.LESS_THAN, aggregateField);
    		if(p.filter(tup)){
        		groupedTuple.setField(afield, new IntField(tup.getField(afield).hashCode()));
    		}
    		break;
    	case MAX:
    		Predicate p1 = new Predicate(afield, Predicate.Op.GREATER_THAN, aggregateField);
    		if(p1.filter(tup)){
        		groupedTuple.setField(afield, new IntField(tup.getField(afield).hashCode()));
    		}
    	}
    	
   		groupedTuples.put(groupByFiled, groupedTuple);
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
