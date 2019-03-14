package AverageTemperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class Descending implements WritableComparable {

	private int value;

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public void setValue(String value) {
		this.value = Integer.parseInt(value);
	}

	Descending() {

	}

	Descending(String s) {
		this.value = Integer.parseInt(s);
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		value = arg0.readInt();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(value);

	}

	@Override
	public int hashCode() {
		return Objects.hash(value);
	}

	@Override
	public boolean equals(Object obj) {
		if (((Descending)obj).value == this.value) {
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}

	@Override
	public int compareTo(Object o) {
        int thisValue = this.value;
        int thatValue = ((Descending)o).value;
        return (thisValue < thatValue ? 1 : (thisValue==thatValue ? 0 : -1));
      }
}
