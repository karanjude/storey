package simpledb;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {

    private final PageId pid;
	private final int tupleno;

	/** Creates a new RecordId refering to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
		this.pid = pid;
		this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
    	return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
    	return this.pid;
    }
    
    @Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof RecordId)) {
			return false;
		}
		RecordId other = (RecordId) obj;
		if (pid == null) {
			if (other.pid != null) {
				return false;
			}
		} else if (!pid.equals(other.pid)) {
			return false;
		}
		if (tupleno != other.tupleno) {
			return false;
		}
		return true;
	}
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pid == null) ? 0 : pid.hashCode());
		result = prime * result + tupleno;
		return result;
	}
    
}
