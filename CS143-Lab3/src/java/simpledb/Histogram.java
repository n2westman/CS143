package simpledb;

public interface Histogram {
//  public void addValue(int v);
//  public double estimateSelectivity(Predicate.Op op, int v);
  public double avgSelectivity();
  public String toString();
}