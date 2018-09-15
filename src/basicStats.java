import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class basicStats {

	public static class Map extends
			Mapper<LongWritable, Text, Text, FloatWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Dummy value for key
			Text word = new Text("a");
			
			FloatWritable one = new FloatWritable(Float.parseFloat(value.toString()));
			context.write(word, one);
		}
	}

	public static class Reduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {

			float sum = 0, max_no = 0, min_no = 1, count = 0, avg = 0;
			double sum_sd = 0;

			ArrayList<Float> tmp_data = new ArrayList<Float>();
			ArrayList<Double> tmp_sd = new ArrayList<Double>();

			FloatWritable result1 = new FloatWritable();
			FloatWritable result2 = new FloatWritable();
			FloatWritable result3 = new FloatWritable();
			FloatWritable result4 = new FloatWritable();
			FloatWritable result5 = new FloatWritable();

			for (FloatWritable val : values) {
				float tmp_val = val.get();
				tmp_data.add(tmp_val);
				
				sum += tmp_val;
				count++;
				if (max_no < tmp_val) {
					max_no = tmp_val;
				}
				if (min_no > tmp_val) {
					min_no = tmp_val;
				}
			}
			avg = sum / count;
			for (int i= 0 ;i<count;i++) {
				float tmp_val = tmp_data.get(i);
				tmp_sd.add(Math.pow((tmp_val - avg), 2));
			}

			for (int i = 0; i < count; i++) {
				sum_sd += tmp_sd.get(i);
			}
			sum_sd = (sum_sd / count);
			sum_sd = Math.sqrt(sum_sd);
			
			Text key1 = new Text("sum");
			Text key2 = new Text("max");
			Text key3 = new Text("min");
			Text key4 = new Text("avg");
			Text key5 = new Text("standard deviation");

			result1.set(sum);
			result2.set(max_no);
			result3.set(min_no);
			result4.set(avg);
			result5.set((float) sum_sd);

			context.write(key1, result1);
			context.write(key2, result2);
			context.write(key3, result3);
			context.write(key4, result4);
			context.write(key5, result5);

		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		// To delete output folder automatically
		File file = new File("/root/MoocHomeworks/basicStatistics/output");
		if (file.isDirectory()) {
			if (file.list().length != 0) {
				String files[] = file.list();
				for (String temp : files) {
					File fileDelete = new File(file, temp);
					fileDelete.delete();
				}
			}

			if (file.list().length == 0) {
				file.delete();
			} else {
				System.out.println("cannot delete folder");
			}
		} else {
			System.out.println("No output directory exist");
		}

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: basicStats <in> <out>");
			System.exit(2);
		}

		// create a job with name "basicStats"
		Job job = new Job(conf, "basicStats");
		job.setJarByClass(basicStats.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// Add a combiner here, not required to successfully run the wordcount
		// program

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(FloatWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
