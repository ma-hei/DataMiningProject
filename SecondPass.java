package hadoop_project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

public class SecondPass {

	public static class SecondPassMapper extends
			Mapper<Object, BytesWritable, Text, IntWritable> {

		private static IntWritable count;
		private Text word = new Text();
		public ArrayList<HashSet<Integer>> candidateItemsets = new ArrayList<HashSet<Integer>>();

		public void setup(Context context) throws IOException {

			Path path = context.getWorkingDirectory();
			String firstPassOutputFile = path.toString();
			firstPassOutputFile = firstPassOutputFile
					.concat("/temp_candidates/part-r-00000");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path inFile = new Path(firstPassOutputFile);
			FSDataInputStream in = fs.open(inFile);
			
			String line;
			String[] temp = new String[2];
			String tempString;
			String[] itemset;
			HashSet<Integer> tempSet;
			while ((line = in.readLine()) != null) {

				temp = line.split("\\t");

				tempString = temp[0].substring(1, temp[0].length());
				tempString = tempString.substring(0, tempString.length() - 1);

				itemset = tempString.split(", ");
				tempSet = new HashSet<Integer>();
				for (String item : itemset) {
					tempSet.add(Integer.parseInt(item));
				}
				System.out.println(tempSet + " added");
				candidateItemsets.add(tempSet);

			}
			in.close();
		}

		public void map(Object key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			HashSet<Integer> currentBasket = new HashSet<Integer>();
			HashMap<HashSet<Integer>, Integer> candidateCounts = new HashMap<HashSet<Integer>, Integer>();

			for (HashSet<Integer> candidate : candidateItemsets) {
				candidateCounts.put(candidate, 0);
			}

			String str;
			int size = value.getLength();
			InputStream is = null;
			byte[] b = new byte[size];
			is = new ByteArrayInputStream(value.getBytes());
			is.read(b);
			str = new String(b);
			is.close();

			String[] baskets = str.split("\n");
			Log.info(baskets.length + " baskets handed to this map task");
			
			String[] itemsAsString;

			for (String basket : baskets) {

				
				itemsAsString = basket.split(" ");
				currentBasket = new HashSet<Integer>();
				for (String item : itemsAsString) {
					currentBasket.add(Integer.parseInt(item));
				}

				boolean basketContainsCandidate = true;
				for (HashSet<Integer> candidate : candidateItemsets) {

					basketContainsCandidate = true;
					for (Integer item : candidate) {

						if (!currentBasket.contains(item)) {
							basketContainsCandidate = false;
							break;
						}

					}
					if (basketContainsCandidate) {
						// System.out.println(candidate+"; "+currentBasket);
						candidateCounts.put(candidate,
								candidateCounts.get(candidate) + 1);
					}

				}

			}

			Set<HashSet<Integer>> keys = candidateCounts.keySet();

			for (HashSet<Integer> key2 : keys) {
				word.set(key2.toString());
				count = new IntWritable(candidateCounts.get(key2).intValue());
				context.write(word, count);
			}
		}
	}

	public static class SecondPassCombiner extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			System.out.println(key.toString() + "; " + result.toString());
			context.write(key, result);
		}
	}

	public static class SecondPassReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private int threshold = 0;

		public void setup(Context context) throws IOException {

			Path path = context.getWorkingDirectory();
			String pathAsString = path.toString();
			pathAsString = pathAsString
					.concat("/config/threshold_whole_file.txt");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path inFile = new Path(pathAsString);
			FSDataInputStream in = fs.open(inFile);
			String line;
			String[] temp = new String[2];
			String thresholdAsString = null;
			while ((line = in.readLine()) != null) {
				temp = line.split(":");
				thresholdAsString = temp[1];
			}
			in.close();
			threshold = Integer.parseInt(thresholdAsString);

		}

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			if (sum >= threshold)
				context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(SecondPass.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapperClass(SecondPassMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(SecondPassReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

	}

}
