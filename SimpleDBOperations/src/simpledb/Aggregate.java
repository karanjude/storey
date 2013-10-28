package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

    private final DbIterator child;
	private final int afield;
	private final int gfield;
	private final Op aop;
	private Aggregator aggregator;
	private Type aggType;
	private DbIterator iterator;

	/**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	TupleDesc aggFieldTupleDesc = child.getTupleDesc();
    	aggType = aggFieldTupleDesc.getType(afield);
    	
    	switch(aggType){
    	case INT_TYPE:
    		aggregator = new IntAggregator(gfield, Type.INT_TYPE, afield, aop);
    		break;
    	case STRING_TYPE:
    		aggregator = new StringAggregator(gfield, Type.STRING_TYPE, afield, aop);
    		break;
    	}
    	
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;
		iterator = aggregator.iterator();
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
    	while(child.hasNext()){
        	Tuple tupleToMerge = child.next();
        	aggregator.merge(tupleToMerge);
    	}
    	
    	iterator.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
    	Tuple tupleToReturn = iterator.next();
    	return tupleToReturn;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
    	Type[] types;
    	String[] strings;
    	
    	if(gfield == Aggregator.NO_GROUPING){
    		types = new Type[]{
    				aggType
    		};
    		strings = new String[]{
    			aggName(aop)	
    		};
    	}else{
    		types = new Type[]{
    				Type.INT_TYPE,
    				aggType
    		};
    		strings = new String[]{
    				"",
    				aggName(aop)
    		};
    	}
    	
		TupleDesc mergedTupleDesc = new TupleDesc(
				types,
				strings
				);
        return mergedTupleDesc;
    }

    public void close() {
    	iterator.close();
    }
    
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
    	return iterator.hasNext();
    }
}
