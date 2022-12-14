import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ExName {
  	public static class Exmapclass extends Mapper<LongWritable, Text, Text, IntWritable>{
  		private Text stock_id=new Text();
  		private DoubleWritable High = new DoubleWritable();
  		public void Map(LongWritable key,Text value,Context cont)throws Exception{
  			String[] m = value.toString().split(",");
  			double high=Double.parseDouble(m[4]);
  			High.set(high);
  			cont.write(stock_id,High);
  		}
}

public static class Exredclass  extends Reducer<Text,IntWritable,Text,IntWritable> {
		private DoubleWritable ress = new DoubleWritable();
		public void Reduce(Text key,Iterable<DoubleWritable> value,Context cont)throws Exception{
	
	
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, " ");
    job.setJarByClass();
    job.setMapperClass();
    job.setReducerClass();
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass();
    job.setMapOutputValueClass();
    job.setOutputKeyClass();
    job.setOutputValueClass(;
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
}
