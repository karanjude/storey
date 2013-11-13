package simpledb;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import javax.sound.sampled.ReverbType;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private int numFields;
	private final Type[] typeAr;
	private final String[] fieldAr;
	private Map<String, Integer> reverseIndexLookup = new HashMap<String, Integer>();
	
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
    	int n = td1.typeAr.length + td2.typeAr.length;
    	Type[] types = new Type[n];
    	String[] strings = new String[n];
    	
    	int ctr = 0;
    	
    	for(int i = 0;i < td1.numFields;i++){
    		types[ctr] =  td1.typeAr[i];
    		if(null != td1.fieldAr)
    			strings[ctr] = td1.fieldAr[i];
    		ctr++;
    	}
    	
    	for(int i = 0;i < td2.numFields;i++){
    		types[ctr] = td2.typeAr[i];
    		if(null != td2.fieldAr)
    			strings[ctr] = td2.fieldAr[i];
    		ctr++;
    	}
    	
    	TupleDesc tupleDesc = new TupleDesc(types, strings);
        return tupleDesc;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
		this.typeAr = typeAr;
		this.fieldAr = fieldAr;
		this.numFields = typeAr.length;

		if(null != fieldAr){
			for(int i = 0;i < fieldAr.length;i++){
				this.reverseIndexLookup.put(fieldAr[i], i);
			}
		}
		
		
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	return this.fieldAr[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
    	if(this.reverseIndexLookup.containsKey(name)){
    		return this.reverseIndexLookup.get(name);
    	}
    	
    	throw new NoSuchElementException(name);
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
    	return this.typeAr[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for(int i = 0; i < typeAr.length; i++){
    		size += typeAr[i].getLen();
    	}
        return size;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(fieldAr);
		result = prime * result + numFields;
		result = prime * result + Arrays.hashCode(typeAr);
		return result;
	}
    
    public boolean equals(Object o) {
    	if(!(o instanceof TupleDesc)){
    		return false;                                                                                                                           
    	}

	    TupleDesc other = (TupleDesc)o;                                                                                                         
	
	    if(this.numFields != other.numFields)
	    	return false;                                                                                                                           

	    for(int i = 0; i < this.numFields;i++){                                                                                                 
	            if(!this.typeAr[i].equals(other.typeAr[i]))
	            return false;                                                                                                                   
	     }

	    return true;                                                                                                                        
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
