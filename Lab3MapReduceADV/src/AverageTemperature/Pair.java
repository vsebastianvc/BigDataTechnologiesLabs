package AverageTemperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

  private double first;
  private int second;
  
  public Pair() {
  }
  
  public Pair(double first, int second) {
    set(first, second);
  }
  
  public void set(double first, int second) {
    this.first = first;
    this.second = second;
  }
  
  public double getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(first);
    out.writeInt(second);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readDouble();
    second = in.readInt();
  }
  
  @Override
  public int hashCode() {
    return (int) (first * 163 + second);
  }
  
  @Override
  public boolean equals(Object o) {
    if (o.getClass().equals(Pair.class)) {
    	Pair ip = (Pair) o;
      return first == ip.first && second == ip.second;
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(Pair ip) {
    int cmp = compare(first, ip.first);
    if (cmp != 0) {
      return cmp;
    }
    return compare(second, ip.second);
  }
  

  public static int compare(double a, double b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }
  
}
