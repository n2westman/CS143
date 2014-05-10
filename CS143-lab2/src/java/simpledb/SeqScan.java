package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Is the scan open?
     */
    private boolean m_isOpen = false;
    private TransactionId m_tid;
    
    /**
     * The tuple description of the tuples being returned by this scan.
     */
    private TupleDesc m_td;
    
    /**
     * An iterator over the tuples in this table.
     */
    private transient DbFileIterator m_it;
    
    /**
     * The name of the table being scanned
     */
    private String m_tablename;
    
    /**
     * The alias of the table.
     */
    private String m_alias;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        m_tid = tid;
        reset(tableid,tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
    	return m_tablename;
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {   
    	// some code goes here
    	return m_alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        m_isOpen=false;
        m_alias = tableAlias;
        m_tablename = Database.getCatalog().getTableName(tableid);
        m_it = Database.getCatalog().getDatabaseFile(tableid).iterator(m_tid);
        m_td = Database.getCatalog().getTupleDesc(tableid);
        
        
        // Create TupleDesc using tableAlias 
        String[] names = new String[m_td.numFields()];
        Type[] types   = new Type[m_td.numFields()];
        for (int i = 0; i < m_td.numFields(); i++) {                     
            names[i] = tableAlias + "." + m_td.getFieldName(i);
            types[i] = m_td.getFieldType(i);
        }
        m_td = new TupleDesc(types, names);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        if (m_isOpen)
            throw new DbException("double open on one DbIterator.");

        m_it.open();
        m_isOpen = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here        
        return m_td;

    }

    /**     
     * @throws IllegalStateException If !isOpen().
     */
    protected void checkOpen() throws IllegalStateException {
    	if (!m_isOpen) {
            throw new IllegalStateException("iterator is closed");    	
    	}
    }

    /**
     * @return true If more Tuples exist. 
     */
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        checkOpen();
    	return m_it.hasNext();
    }

    /**
     * @return The next Tuple.
     */
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	checkOpen();
        return m_it.next();
    }

    /**
     * Close the iterator.
     */
    public void close() {
        // some code goes here
        m_it.close();
        m_isOpen = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }
}
