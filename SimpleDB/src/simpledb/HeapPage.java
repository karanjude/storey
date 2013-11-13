package simpledb;

import java.util.*;
import java.io.*;

/**
 * HeapPage stores pages of HeapFiles and implements the Page interface that
 * is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    HeapPageId pid;
    TupleDesc td;
    byte header[];
    Tuple tuples[];
    int numSlots;
    
    List<Integer> slotsFilled = new ArrayList<Integer>();
	List<Integer> deletedSlots = new ArrayList<Integer>();


    byte[] oldData;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.PAGE_SIZE*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#PAGE_SIZE
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        try{
            // allocate and read the actual records of this page
            tuples = new Tuple[numSlots];
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {     
    	int tupleSize = this.td.getSize();
    	int nrecords = (BufferPool.PAGE_SIZE * 8) /  (tupleSize * 8 + 1);  //floor comes for free
    	return nrecords;

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() { 
    	 int nrecords = getNumTuples();
    	 int nheaderbytes = (nrecords / 8);
    	    if (nheaderbytes * 8 < nrecords)
    	        nheaderbytes++;  //ceiling
    	return nheaderbytes;
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            return new HeapPage(pid,oldData);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        oldData = getPageData().clone();
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    	return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!getSlot(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        slotsFilled.add(slotId);
        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.PAGE_SIZE;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!getSlot(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
            	if(null == tuples[i])
            		System.out.println("null");
            	
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @param tableid The id of the table that this empty page will belong to.
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.PAGE_SIZE;
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public synchronized void deleteTuple(Tuple t) throws DbException {
    	if(!getId().equals(t.getRecordId().getPageId()))
    		throw new DbException("Record Id Mismatch");
    	
    	int tno = t.getRecordId().tupleno();
    	
    	if(!getSlot(tno))
    		throw new DbException("Trying to delete an empty slot");
    	
    	if(!slotsFilled.contains(tno))
    		throw new DbException("Trying to delete a non existant tuple");
    		
    	setSlot(tno, false);
    	tuples[tno] = null;
    	
    	int indexToRemove = slotsFilled.indexOf(tno);
    	slotsFilled.set(indexToRemove, -1);
    	deletedSlots.add(indexToRemove);
    	//slotsFilled.remove(indexToRemove);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public synchronized void addTuple(Tuple t) throws DbException {
    	if(getNumEmptySlots() == 0)
    		throw new DbException("Page Full, no empty slots available");
    	
    	if(!td.equals(t.getTupleDesc()))
    		throw new DbException("Tuple Description misatch");

    	int index = indexOfEmptySlot();
    	
    	t.setRecordId(new RecordId(this.getId(), index));

    	tuples[index] = t;
    	setSlot(index, true);
    }

    private int indexOfEmptySlot() {
    	int indexToReturn = 0;
    	
    	for (int i = 0; i < header.length; i++, indexToReturn += 8) {
    		byte b = header[i];
    		byte r = ~0;
    		if((b & r) != r){
    			for(int bi = 0;bi < 8;bi++)
    				if(!isSlotFilled(bi, b))
    					return indexToReturn + bi;
    		}
		}
    	
		return 0;
	}

	private int freeOffset(byte b) {
		int result = 8 + 1;

		for (int i = 8; i >= 1; i--) {
			if(!getBit(b, i)){
				result -= i;
				break;
			}
		}
		
		return result;
	}

	private boolean getBit(byte b, int bitNumber) {
		return (b & (1 << bitNumber - 1)) != 0;
	}

	private TransactionId dirtyTranscation = null;
    
    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
    	if(dirty)
    		dirtyTranscation = tid;
    	else
    		dirtyTranscation = null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
    	return dirtyTranscation;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
    	return tuples.length - slotsFilled.size() + deletedSlots.size();
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean getSlot(int i) {
    	if(i > getNumTuples())
    		return false;
    	
    	return isSlotFilled(i);
    }

    private boolean isSlotFilled(int index) {
    	int indexOfByteHeaderToCheck = index / 8;
    	int offsetOfByteHeaderToCheck = index % 8;
    	
    	byte headerByte = header[indexOfByteHeaderToCheck];
    	return isSlotFilled(offsetOfByteHeaderToCheck, headerByte);
	}

	private boolean isSlotFilled(int offsetOfByteHeaderToCheck, byte headerByte) {
		if((headerByte & (1 << offsetOfByteHeaderToCheck)) > 0)
    		return true;
		return false;
	}

	/**
     * Abstraction to fill or clear a slot on this page.
     */
    private void setSlot(int index, boolean value) {
    	int indexOfByteHeaderToCheck = index / 8;
    	int offsetOfByteHeaderToCheck = index % 8;
    	
    	byte headerByte = header[indexOfByteHeaderToCheck];
    	byte r = (byte) (1 << offsetOfByteHeaderToCheck);
    	byte newByte = (byte) (headerByte | r);
    	
    	if(!value){
    		byte ll = headerByte;
    		//ystem.out.format("0x%x ", ll);
    		byte rr = (byte) ~r;
    		//System.out.format("0x%x ", rr);
    		byte res =  (byte) (ll & rr);
    		//System.out.format("0x%x ", res);
    		newByte = (byte) res;
    		//System.out.format("0x%x ", newByte);
    	}
    	
    	header[indexOfByteHeaderToCheck] = newByte;
    	
    	if(value)
    		slotsFilled.add(index);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
    	return new HeapPageIterator(this);
    }

	public synchronized Tuple readNextTuple(HeapPageIterator heapPageIterator) {
		int indx = slotsFilled.get(heapPageIterator.ctr);
		
		while(indx == -1 && heapPageIterator.hasNext()){
			heapPageIterator.ctr++;
			indx = slotsFilled.get(heapPageIterator.ctr);
		}
		
		if(-1 == indx)
			heapPageIterator.ctr++;
			
		Tuple toReturn = tuples[heapPageIterator.ctr];
		heapPageIterator.ctr++;
		
		if(null == toReturn){
			if(heapPageIterator.hasNext())
				return heapPageIterator.next();
		}
		
		return toReturn;
	}

}

