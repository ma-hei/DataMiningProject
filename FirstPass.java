package hadoop_project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.mortbay.log.Log;

public class FirstPass {

	public static class TokenizerMapper extends
			Mapper<Object, BytesWritable, Text, IntWritable> {
		private int threshold;
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void setup(Context context) throws IOException {

			Path path = context.getWorkingDirectory();
			String configFile = path.toString();
			configFile = configFile.concat("/config/threshold_chunk.txt");
			Path configFilePath = new Path(configFile);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(configFilePath);
			String line;
			String[] temp = new String[2];
			String thresholdString = null;
			while ((line = in.readLine()) != null) {
				temp = line.split(":");
				thresholdString = temp[1];
			}
			in.close();
			this.threshold = Integer.parseInt(thresholdString);
		}

		public void map(Object key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String str;
			int size = value.getLength();
			InputStream is = null;
			byte[] b = new byte[size];
			is = new ByteArrayInputStream(value.getBytes());
			is.read(b);
			str = new String(b);
			String[] buckets = str.split("\n");
			is.close();
			Apriori apriori = new Apriori();

			ArrayList<ArrayList<HashSet<Integer>>> frequentItemsets = apriori
					.aprioriCalculation(buckets, this.threshold);

			for (ArrayList<HashSet<Integer>> itemsetSize : frequentItemsets) {
				for (HashSet<Integer> itemset : itemsetSize) {
					Log.info(itemset.toString());
					word.set(itemset.toString());
					context.write(word, one);
				}
			}

		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			result.set(1);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(FirstPass.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
