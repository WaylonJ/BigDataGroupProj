

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
	private final static IntWritable one = new IntWritable(1);

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void map(LongWritable _key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String TempString = value.toString();
		String[] SingleBookData = TempString.split(",");
		
		try {		
			String together = SingleBookData[10];
			output.collect(new Text(together), one);
			
			IntWritable myDelay = new IntWritable(Integer.parseInt(SingleBookData[14].toString()));
			String total = together + "_ArrDelayMins";
			output.collect(new Text(total), myDelay);
			
		} catch (Exception e) {
			System.out.println("oops");
		}
		


	}
}
public static class BookNewReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void reduce(Text _key,
			Iterator<IntWritable> values,
			OutputCollector<Text,IntWritable> output, 
			Reporter reporter)
			throws IOException {
		Text key = _key;
		int frequencyForYear = 0;
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			IntWritable value = (IntWritable) values.next();
			frequencyForYear += value.get();
			// process value
		}
		output.collect(key, new IntWritable(frequencyForYear));
	}
}
	public static void main(String[] args) {
		JobClient client = new JobClient();
		// Configurations for Job set in this variable
		JobConf conf = new JobConf(BookNew.class);
		
		// Name of the Job
		conf.setJobName("BookNew");
		
		// Data type of Output Key and Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// Setting the Mapper and Reducer Class
		conf.setMapperClass(BookNewMapper.class);
		conf.setReducerClass(BookNewReducer.class);

		// Formats of the Data Type of Input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// Specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		client.setConf(conf);
		try {
			// Running the job with Configurations set in the conf.
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
