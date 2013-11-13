package simpledb;

import java.util.Iterator;

public class HeapPageIterator implements Iterator<Tuple> {

	private final HeapPage heapPage;
	public int ctr = 0;

	public HeapPageIterator(HeapPage heapPage){
		this.heapPage = heapPage;
	}
	
	@Override
	public synchronized boolean hasNext() {
		int numSlots = heapPage.slotsFilled.size() - heapPage.deletedSlots.size();
		boolean reachedEnd = numSlots > 0 && ctr < heapPage.slotsFilled.size();
		return reachedEnd;
	}

	@Override
	public synchronized Tuple next() {
		return heapPage.readNextTuple(this);
	}

	@Override
	public void remove() {
	}

}
