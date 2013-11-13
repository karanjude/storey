package simpledb;

import simpledb.Aggregator.Op;

public class NoGroupingStrategy extends GroupingStrategy {

	public NoGroupingStrategy(int gbfield, int afield, Op what, Type gbfieldtype) {
		super(gbfield, afield, what, gbfieldtype);
	}

	public void merge(Tuple tup) {
		Type types[] = new Type[]{ Type.INT_TYPE };
		TupleDesc mergedTupleDesc = new TupleDesc(
				types
				);
		Tuple mergedTuple = new Tuple(mergedTupleDesc);
		Field grpField = new IntField(-1);
		
    	if(!groupedTuples.containsKey(grpField)){
    		if(!what.equals(Op.COUNT))
    			mergedTuple.setField(0, new IntField(tup.getField(afield).hashCode()));
    		else
    			mergedTuple.setField(0, new IntField(1));
    		groupedTuples.put(grpField, mergedTuple);
    	}else{
    		updateGroupedTuples(grpField, tup);
    	}

	}
	
	
    private void updateGroupedTuples(Field groupByFiled, Tuple tup) {
    	Tuple groupedTuple = groupedTuples.get(groupByFiled);
    	IntField aggregateField = (IntField) groupedTuple.getField(0);
    	
    	switch(what){
    	case COUNT:
    		int cnt = 1 + groupedTuple.getField(0).hashCode();
    		groupedTuple.setField(0, new IntField(cnt));
    		break;
    	case SUM:
    		int sum = tup.getField(0).hashCode() + groupedTuple.getField(0).hashCode();
    		groupedTuple.setField(0, new IntField(sum));
    		break;
    	case AVG:
    		if(!groupedTuplesCount.containsKey(groupByFiled)){
    			groupedTuplesCount.put(groupByFiled, 1);
    		}
    		
    		int prevCount = groupedTuplesCount.get(groupByFiled);
    		int prevAvg = aggregateField.getValue();
    		int prevSum = prevAvg * prevCount;
    		int newVal = tup.getField(afield).hashCode();
    		int sum1 = newVal + prevSum;
    		int cnt1 = groupedTuplesCount.get(groupByFiled) + 1;
    		groupedTuplesCount.put(groupByFiled, cnt1);
    		
    		int avg = (int)(sum1/cnt1);
    		//if(0 == avg)
    		//	System.out.println("0 avg");
    		
    		
    		groupedTuple.setField(0, new IntField(avg));
    		break;
    	case MIN:
    		Predicate p = new Predicate(0, Predicate.Op.LESS_THAN, aggregateField);
    		if(p.filter(tup)){
        		groupedTuple.setField(0, new IntField(tup.getField(0).hashCode()));
    		}
    		break;
    	case MAX:
    		Predicate p1 = new Predicate(0, Predicate.Op.GREATER_THAN, aggregateField);
    		if(p1.filter(tup)){
        		groupedTuple.setField(0, new IntField(tup.getField(0).hashCode()));
    		}
    	}
    	
    	//System.out.println("Calculated avg value: " + groupedTuple);
    	
   		groupedTuples.put(groupByFiled, groupedTuple);
	}


}
