package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram{

    private int twidth;
    private double bwidth;
    private int min;
    private int max;
    private int nbuckets;
    private int ntuples;
    private int[] buckets;
    
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
    	this.max = max;
    	ntuples = 0;
    	twidth = this.max - this.min + 1;
    	
    	if(buckets >= twidth)
    	{
	  nbuckets = twidth;
	  bwidth = 1.0;
    	}
    	else
    	{
	  nbuckets = buckets;
	  bwidth = (double) twidth / buckets;
    	}
    	
    	this.buckets = new int[nbuckets];
    	
//    	System.out.printf("%d,%d,%d,%d,%d,%f\n", this.min, this.max, ntuples, twidth, nbuckets, bwidth);
    }

    private boolean outOfRange(int v) {
      return v < min || v > max;
    }
    
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
	if(outOfRange(v))
	  return;
    	int index =  (int) ((v - min) / bwidth);
    	
    	ntuples++;
    	buckets[index]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
	int index =  (int) ((v - min) / bwidth);
	
	if(op == Predicate.Op.EQUALS) {
	  if(outOfRange(v))
	    return 0.0;
	  return buckets[index] / bwidth / ntuples;
	} if(op == Predicate.Op.NOT_EQUALS) {
	  return 1.0 - estimateSelectivity(Predicate.Op.EQUALS,v);
	} if(op == Predicate.Op.GREATER_THAN) {
	  if(v > max) return 0.0;
	  if(v < min) return 1.0;
	  double retval = 0.0;
	  int i;
	  for(i = nbuckets - 1; i > index; i--)
	    retval += buckets[i] / bwidth / ntuples;
	  return retval;
	} if(op == Predicate.Op.LESS_THAN) {
	  return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v) - estimateSelectivity(Predicate.Op.EQUALS, v);
	} if(op == Predicate.Op.GREATER_THAN_OR_EQ) {
	  return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
	} if(op == Predicate.Op.LESS_THAN_OR_EQ) {
	  return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
	}
	
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        int sum = 0;
        for(int i = 0; i < nbuckets; i++)
	  sum += buckets[i] * buckets[i];
        return (double) sum / (ntuples * ntuples);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
