import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class Question3 {
    // this class implements question3

    public static class TokenMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text bussId = new Text();
        private DoubleWritable rate = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("::");
            if (split.length != 4) {
                // this record is in a wrong format, discard it
                return;
            }
            bussId.set(split[2]);
            rate.set(Double.valueOf(split[3]));
            context.write(bussId, rate);
        }

    }
// calculate the average
    public static class AvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable avg = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            BigDecimal total = BigDecimal.ZERO;
            BigDecimal count = BigDecimal.ZERO;
            for (DoubleWritable val : values) {
                total = total.add(BigDecimal.valueOf(val.get()));
                count = count.add(BigDecimal.ONE);
            }
            if (!count.equals(BigDecimal.ZERO)) {
                avg.set(total.divide(count, BigDecimal.ROUND_HALF_UP).doubleValue());
                context.write(key, avg);
            }
        }
    }
// sort to find the top 10
    public static class SortMapper extends Mapper<Object, Text, NullWritable, BussWritable> {
        private static final int K = 10;
        private Queue<BussWritable> q = new PriorityQueue<BussWritable>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\t");
            if (split.length != 2) {
                // this record is in a wrong format, discard it
                return;
            }
            Text bussId = new Text(split[0].trim());
            DoubleWritable rate = new DoubleWritable(Double.valueOf(split[1].trim()));
            BussWritable b = new BussWritable(bussId, rate);
            if (!q.contains((Object)b)) {
//            if (!has) {
                q.offer(b);
                if (q.size() > K) {
                    q.poll();
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            while (!q.isEmpty()) {
                BussWritable b = q.poll();
                context.write(NullWritable.get(), b);
            }
        }

    }

    public static class SortReducer extends Reducer<NullWritable, BussWritable, Text, DoubleWritable> {
        private static final int K = 10;
        private Queue<BussWritable> q = new PriorityQueue<BussWritable>();

        public void reduce(NullWritable key, Iterable<BussWritable> values, Context context) throws IOException, InterruptedException {
            for (BussWritable b : values) {
                BussWritable w = BussWritable.deepcopy(b);
                q.offer(w);
                if (q.size() > K) {
                    q.poll();
                }
            }
            while (!q.isEmpty()) {
                BussWritable b = q.poll();
                context.write(b.bussId, b.rate);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> buss = new ArrayList<Text>();
            List<Text> sort = new ArrayList<Text>();
            for (Text t : values) {
                if (t.toString().startsWith("buss::")) {
                    Text bussInfo = new Text(t.toString().replaceFirst("buss::", ""));
                    if (!buss.contains(bussInfo)) {
                        buss.add(bussInfo);
                    }
                } else if (t.toString().startsWith("sort::")) {
                    Text sortInfo = new Text(t.toString().replaceFirst("sort::", ""));
                    if (!sort.contains(sortInfo)) {
                        sort.add(sortInfo);
                    }
                } else {
                    // invalid record, discard it
                }
            }
            for (Text b : buss) {
                for (Text s : sort) {
                    context.write(key, new Text(b.toString() + "\t" + s.toString()));
                }
            }

        }
    }
    public static class BusiMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("::");
            if (split.length != 3) {
                // this record is in a wrong format, discard it
                return;
            }
            Text bussId = new Text(split[0].trim());
            Text otherInfo = new Text("buss::" + split[1].trim() + "\t" + split[2].trim());
            context.write(bussId, otherInfo);
        }
    }

    public static class RankMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\t");
            if (split.length != 2) {
                // this record is in a wrong format, discard it
                return;
            }
            Text bussId = new Text(split[0].trim());
            Text otherInfo = new Text("sort::" + split[1].trim());
            context.write(bussId, otherInfo);
        }
    }


    public static void main(String[] args) throws Exception {
        // usage: input/review.csv input/business.csv output/
        String reviewPath = args[0];
        String bussinessPath = args[1];
        String outputPath = args[2];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average");
        // average job
        job.setJarByClass(Question3.class);
        job.setMapperClass(TokenMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(reviewPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/average"));
        job.waitForCompletion(true);
        // sort job
        Job job2 = Job.getInstance(conf, "Sort");
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(BussWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path(outputPath + "/average/part-*"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/sort"));
        job2.waitForCompletion(true);
        // join job
        Job job3 = Job.getInstance(conf, "Join");
        MultipleInputs.addInputPath(job3, new Path(bussinessPath), TextInputFormat.class, BusiMapper.class);
        MultipleInputs.addInputPath(job3, new Path(outputPath + "/sort/part-*"), TextInputFormat.class, RankMapper.class);
        FileOutputFormat.setOutputPath(job3, new Path(outputPath + "/join"));
        job3.setReducerClass(JoinReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}