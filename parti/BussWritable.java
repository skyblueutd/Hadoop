import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class BussWritable implements WritableComparable<BussWritable> {
    public Text bussId = new Text();
    public DoubleWritable rate = new DoubleWritable();

    public BussWritable(Text b, DoubleWritable d) {
        this.bussId = b;
        this.rate = d;
    }

    public BussWritable() {
        bussId = new Text();
        rate = new DoubleWritable();
    }

    @Override
    public int compareTo(BussWritable o) {
        return rate.compareTo(o.rate);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        bussId.write(dataOutput);
        rate.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        bussId.readFields(dataInput);
        rate.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof BussWritable) {
            BussWritable b = (BussWritable) o;
            if (this.bussId.equals(b.bussId) && this.rate.equals(b.rate)) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public static BussWritable deepcopy(BussWritable o) {
        BussWritable b = new BussWritable();
        b.bussId = new Text(o.bussId.toString());
        b.rate = new DoubleWritable(o.rate.get());
        return b;
    }

//    public static void main(String[] argus) {
//        BussWritable a = new BussWritable();
//        a.bussId = new Text("111");
//        a.rate = new DoubleWritable(23.0);
//        BussWritable b = new BussWritable();
//        System.out.println(a.equals((Object) b));
//    }
}