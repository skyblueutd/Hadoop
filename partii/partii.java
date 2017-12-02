import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.HashSet;

public class Question4 extends Configured implements Tool {

    static class AverageMap extends Mapper<LongWritable,Text,Text,DoubleWritable>{

        private HashSet<String> set = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            readFiles(context.getConfiguration());
        }

        protected void readFiles(Configuration conf){
            FileSystem files = null;
            BufferedReader bfr = null;
            try {
                Path path = new Path(conf.get("buss"));
                files = FileSystem.get(conf);
                bfr = new BufferedReader(new InputStreamReader(files.open(path)));
                String line = null;
                while((line = bfr.readLine())!=null){
                    String[] infos = line.split("::");
                    if(infos[1].contains("Palo Alto")){
                        set.add(infos[0]);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private Text userId = new Text();
        private DoubleWritable rate = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("::");
            if(infos.length!=4){
                return;
            }
            if(set.contains(infos[2].trim())){
                userId.set(infos[1]);
                rate.set(Double.parseDouble(infos[3]));
                context.write(userId,rate);
            }
        }
    }

    static class AverageReduce extends Reducer<Text,DoubleWritable,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            BigDecimal count = BigDecimal.ZERO;
            BigDecimal total = BigDecimal.ZERO;
            for(DoubleWritable value : values){
                count = count.add(BigDecimal.ONE);
                total = total.add(BigDecimal.valueOf(value.get()));
            }
            if(!count.equals(BigDecimal.ZERO)){
                double avg = total.divide(count,BigDecimal.ROUND_HALF_UP).doubleValue();
                context.write(key,new Text(avg+""));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String reviewin = args[0];
        String output = args[1];
        String busicache = args[2];
        Configuration conf = getConf();
        conf.set("buss",busicache);
        Job job1 = Job.getInstance(conf,"avg");
        job1.setJarByClass(Question4.class);
        Path in = new Path(reviewin);
        FileInputFormat.setInputPaths(job1,in);
        Path out = new Path(output);
        FileOutputFormat.setOutputPath(job1,out);
        job1.setMapperClass(AverageMap.class);
        job1.setReducerClass(AverageReduce.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        int res1 = job1.waitForCompletion(true)?0:1;

        return res1;
    }

    public static void main(String [] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),new Question4(),args);
        System.exit(res);
    }
}