/* Reuben Orihuela
 * Waylon Jones
 * Casey Au
 * CS 4650 Final Project
*/

//Import Statements
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.IOException;
import java.util.Iterator;


public class BookNew{
	public static class BookNewMapper extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
	
	//Constant Value of one
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable _key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		//Convert each row to string and establish delimiter
		String TempString = value.toString();
		String[] SingleBookData = TempString.split(",");
		
		try {
			//Add 11th element in CSV to map
			String together = SingleBookData[10];
			output.collect(new Text(together), one);
			
			//Add 15th element in CSV to map
			IntWritable myDelay = new IntWritable(Integer.parseInt(SingleBookData[14].toString()));
			String total = together + "_ArrDelayMins";
			output.collect(new Text(total), myDelay);
			
		} catch (Exception e) {
			System.out.println("oops");
		}
		


	}
}
	public static class BookNewReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text _key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter)
			throws IOException {
			Text key = _key;
			int frequencyForYear = 0;
			while (values.hasNext()) {
				// replace ValueType with actual value type
				IntWritable value = (IntWritable) values.next();
				frequencyForYear += value.get();
			}
			output.collect(key, new IntWritable(frequencyForYear));
		}
	}
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(BookNew.class);
		
		conf.setJobName("BookNew");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(BookNewMapper.class);
		conf.setReducerClass(BookNewReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
