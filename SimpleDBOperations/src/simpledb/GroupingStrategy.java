package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.Aggregator.Op;

public class GroupingStrategy {

	protected final int gbfield;
	protected final int afield;
    protected Map<Field, Tuple> groupedTuples = new HashMap<Field,Tuple>();
    protected Map<Field, Integer> groupedTuplesCount = new HashMap<Field,Integer>();
	protected final Type gbfieldtype;
	protected final Op what;

	public GroupingStrategy(int gbfield, int afield, Op what, Type gbfieldtype) {
		this.gbfield = gbfield;
		this.afield = afield;
		this.what = what;
		this.gbfieldtype = gbfieldtype;
	}

	public void merge(Tuple tup) {
    	Field groupByFiled = tup.getField(gbfield);
    	
    	if(!groupedTuples.containsKey(groupByFiled)){
    		Type types[] = new Type[]{ gbfieldtype, Type.INT_TYPE };
    		TupleDesc mergedTupleDesc = new TupleDesc(
    				types
    				);
    		Tuple mergedTuple = new Tuple(mergedTupleDesc);
    		mergedTuple.setField(gbfield, groupByFiled);
    		if(!what.equals(Op.COUNT))
    			mergedTuple.setField(afield, new IntField(tup.getField(afield).hashCode()));
    		else
    			mergedTuple.setField(afield, new IntField(1));
    		groupedTuples.put(groupByFiled, mergedTuple);
    	}else{
    		updateGroupedTuples(groupByFiled, tup);
    	}

	}
	
	
    private void updateGroupedTuples(Field groupByFiled, Tuple tup) {
    	Tuple groupedTuple = groupedTuples.get(groupByFiled);
    	IntField aggregateField = (IntField) groupedTuple.getField(afield);
    	
    	switch(what){
    	case COUNT:
    		int cnt = 1 + groupedTuple.getField(afield).hashCode();
    		groupedTuple.setField(afield, new IntField(cnt));
    		break;
    	case SUM:
    		int sum = tup.getField(afield).hashCode() + groupedTuple.getField(afield).hashCode();
    		groupedTuple.setField(afield, new IntField(sum));
    		break;
    	case AVG:
    		if(!groupedTuplesCount.containsKey(groupByFiled)){
    			groupedTuplesCount.put(groupByFiled, 1);
    		}

    		int sum1 = tup.getField(afield).hashCode() + (groupedTuplesCount.get(groupByFiled) * aggregateField.getValue());
    		int cnt1 = groupedTuplesCount.get(groupByFiled) + 1;
    		groupedTuplesCount.put(groupByFiled, cnt1);
    		
    		groupedTuple.setField(afield, new IntField(sum1));
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

	public Iterator<Entry<Field, Tuple>> groupedTuplesIterator() {
		return groupedTuples.entrySet().iterator();
	}

}
