package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

	/**
	 * Heap Page Id.
	 */
    final HeapPageId m_heapPageId;
    
    /**
     * Tuple Description of tuples stored in this page.
     */
    final TupleDesc m_td;
    
    /**
     * Indicates which slots in this page have been used.
     * There is one bit per slot, 0 means available, 1 means used. 
     */
    final byte m_header[];
    
    /**
     * The array of tuples stored in this HeapPage.
     */
    final Tuple m_tuples[];
    
    /**
     * The number of slots in this HeapPage.
     */
    final int m_numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.m_heapPageId = id;
        this.m_td = Database.getCatalog().getTupleDesc(id.getTableId());

        this.m_numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        m_header = new byte[getHeaderSize()];
        for (int i=0; i<m_header.length; i++)
            m_header[i] = dis.readByte();
        
        m_tuples = new Tuple[m_numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<m_tuples.length; i++)
                m_tuples[i] = readNextTuple(dis,i);
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
        // some code goes here
        int bitsPerTupleIncludingHeader = m_td.getSize() * 8 + 1;
        int tuplesPerPage = (BufferPool.getPageSize()*8) / bitsPerTupleIncludingHeader; //round down
        return tuplesPerPage;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
    	// some code goes here
    	int tuplesPerPage = getNumTuples();
        int headerBytes = (tuplesPerPage / 8);        
        if (headerBytes * 8 < tuplesPerPage) {
        	headerBytes++;
        }

        return headerBytes;
    }
    
    /** 
     * Return a view of this page before it was modified
     * -- used by recovery 
     */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(m_heapPageId,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    	// some code goes here
    	return m_heapPageId;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        	
    	// if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<m_td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(m_td);
        RecordId rid = new RecordId(m_heapPageId, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<m_td.numFields(); j++) {
                Field f = m_td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

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
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<m_header.length; i++) {
            try {
                dos.writeByte(m_header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<m_tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<m_td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<m_td.numFields(); j++) {
                Field f = m_tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (m_header.length + m_td.getSize() * m_tuples.length); //- numSlots * td.getSize();
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
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
    }

    /**
     * @return the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return null;      
    }

    /**
     * @return the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
    	int count = 0;
        for(int i=0; i<m_numSlots; i++) {
            if(!isSlotUsed(i)) {
                count++;
            }
        }
        return count;

    }

    /**
     * @return true if associated slot on this page is filled, false otherwise.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        int headerBit = i % 8;								// remainder is offset into byte
        int headerByte = (i - headerBit) / 8;				// difference is the byte number
        return (m_header[headerByte] & (1 << headerBit)) != 0;	
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new HeapPageIterator(this);
    }

    /**
     * protected method used by the iterator to get the ith tuple
     * out of this page
     * @param i The index of the tuple to get.
     * @throws NoSuchElementException If the tuple with index i does not exist.
     */
    protected Tuple getTuple(int i) throws NoSuchElementException {

        try {
            if(!isSlotUsed(i)) {                
                return null;
            } else {            
            	return m_tuples[i];
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }
    
    /**
     * Helper class that implements the Java Iterator for tuples on a HeapPage.
     */
    class HeapPageIterator implements Iterator<Tuple> {
        /**
         * The current tuple index.
         */
    	int m_currentIdx = 0;
    	
    	/**
    	 * The next tuple to return.
    	 */
        Tuple m_next = null;
        
        /**
         * The underlying HeapPage.
         */
        HeapPage m_heapPage;

        /**
         * Constructor sets the HeapPage for this iterator
         * @param p The HeapPage to iterate over
         */
        public HeapPageIterator(HeapPage p) {
            m_heapPage = p;
        }

        /**
         * @return true if this iterator has another tuple, false otherwise.
         */
        public boolean hasNext() {
            if (m_next != null) {
                return true;
            }
            try {
                while (true) {
                    m_next = m_heapPage.getTuple(m_currentIdx++);
                    if(m_next != null)
                        return true;
                }
            } catch(NoSuchElementException e) {
                return false;
            }
        }
        
        /**
         * @return The next tuple.
         * @throws NoSuchElementException If no next tuple exists.
         */
        public Tuple next() {
            Tuple next = m_next;

            if (next == null) {
                if (hasNext()) {
                    next = m_next;
                    m_next = null;
                    return next;
                } else {
                    throw new NoSuchElementException();
                }
            } else {
                m_next = null;
                return next;
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    
}
