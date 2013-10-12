package simpledb;

import javax.swing.text.TabExpander;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private final int tableId;
	private final int pgNo;

	/**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
		this.tableId = tableId;
		this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
    	return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageno() {
    	return this.pgNo;
    }

    @Override
	public int hashCode() {
		return this.tableId * 10 + this.tableId;
	}

    @Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof HeapPageId)) {
			return false;
		}
		HeapPageId other = (HeapPageId) obj;
		if (pgNo != other.pgNo) {
			return false;
		}
		if (tableId != other.tableId) {
			return false;
		}
		return true;
	}

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageno();

        return data;
    }

}
