package pkg1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PageRank extends Configured implements Tool {

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException,NullPointerException{
			
			Configuration conf = context.getConfiguration();
			String fKey,fValue;
			
				String[] line = value.toString().split("\t");
				String pageNo = line[0].split(":")[0];
				double rank = Double.parseDouble(line[0].split(":")[1]);
				String sOutlinks= line[1];
				String[] outLinks = sOutlinks.split(",");
				
				String op="", sRank;
				
				for (int i = 0; i < outLinks.length; i++) {
					sRank = String.valueOf(rank/outLinks.length);
					op = pageNo+":" +sRank;
					context.write(new Text(outLinks[i]), new Text(op));
				}
				fKey =pageNo;
				fValue =line[1];
			
			context.write(new Text(fKey), new Text(fValue));

		}

	}

	/* Partitioner Class */
	public static class PartitionerTopBusinesses extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			return numReduceTasks;}
	}

	/* Reducer */
	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double rankK =0.0 , beta =0.8 , rankNiByNi;
			String links = null;
			
			for (Text value : values) {
				if(value.toString().contains(":")){
				rankNiByNi = Double.parseDouble( value.toString().split(":")[1]);
				rankK += rankNiByNi * beta;}
				else{
					links= value.toString();
				}
				//context.write(key, text);
			}

				rankK +=(1-beta)/5;
				String sKey = key.toString() + ":"+rankK ; 
				
				
				context.write(new Text(sKey), new Text(links));
				
				
			}

		}
	

	protected abstract class JobInfo {
		public abstract Class<?> getJarByClass();

		public abstract Class<? extends Mapper> getMapperClass();

		public abstract Class<? extends Reducer> getCombinerClass();

		public abstract Class<? extends Reducer> getReducerClass();

		public abstract Class<?> getOutputKeyClass();

		public abstract Class<?> getOutputValueClass();

	}

	private Job getJobConf(String[] args, Path hdfsPath) throws Exception {

		JobInfo jobInfo = new JobInfo() {
			@Override
			public Class<? extends Reducer> getCombinerClass() {
				return null;
			}

			@Override
			public Class<?> getJarByClass() {
				return PageRank.class;
			}

			@Override
			public Class<? extends Mapper> getMapperClass() {
				return PageRankMapper.class;
			}

			@Override
			public Class<?> getOutputKeyClass() {
				return Text.class;
			}

			@Override
			public Class<?> getOutputValueClass() {
				return Text.class;
			}

			@Override
			public Class<? extends Reducer> getReducerClass() {
				return PageRankReducer.class;
			}

		};
		Job job = setupJob("PageRank", jobInfo, hdfsPath);
		//job.setPartitionerClass(PartitionerTopBusinesses.class);
		// return setupJob("K", jobInfo);
		return job;

	}

	public int run(String[] args) throws Exception {

		int outputCount = 0;
		int nextInputCount = 0;
		Job job;
		long exitPointCounter = 1;
		int iteration = 0;
		
		while (exitPointCounter > 0) {

			String input, output, again_input = null, prev;

			input = args[0];
			output = args[1] + (outputCount + 1);
			
			if (outputCount == 0)
				again_input = args[0];
			else
				again_input = args[1] + nextInputCount + "/part-r-00000";

		
			Path hdfsPath = null;

			job = getJobConf(args, hdfsPath);
			
			if (outputCount == 0) {
				prev = input+ "data.txt" ;

			} else {
				prev = again_input;
			}

			FileInputFormat.setInputPaths(job, new Path(again_input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);
			
			outputCount++;
			nextInputCount++;
			
			if(exitPointCounter>=1){
				
				double rankPrevSum = 0 , rankCurSum =0;
				
				Path ofile = new Path(prev );
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(ofile)));
				String line = br.readLine();
				
				while (line != null) {
				String[] lineArray = line.split("\t");
				rankPrevSum += Double.parseDouble(lineArray[0].split(":")[1]);
				line = br.readLine();
				}
				
				Path cfile = new Path(output+"/part-r-00000" );
				FileSystem fs1 = FileSystem.get(new Configuration());
				BufferedReader br1 = new BufferedReader(new InputStreamReader(
						fs1.open(cfile)));
				String line1 = br1.readLine();
				
				while (line1 != null) {
				String[] lineArray1 = line1.split("\t");
				rankCurSum += Double.parseDouble(lineArray1[0].split(":")[1]);
				line1 = br1.readLine();
				}
				
				double ep = rankCurSum - rankPrevSum;
				
				System.out.println(rankPrevSum);
				System.out.println(rankCurSum);
				System.out.println("ep ======"+ Math.abs(ep));
				if(Math.abs(ep) < .05)
					exitPointCounter--;
			}

		}		

		return 0;
	}

	protected Job setupJob(String jobName, JobInfo jobInfo, Path hdfsPath)
			throws Exception {

		Job job = new Job(new Configuration(), jobName);
		job.setJarByClass(jobInfo.getJarByClass());

		job.setOutputKeyClass(jobInfo.getOutputKeyClass());
		job.setOutputValueClass(jobInfo.getOutputValueClass());

		job.setMapperClass(jobInfo.getMapperClass());

		if (jobInfo.getCombinerClass() != null)
			job.setCombinerClass(jobInfo.getCombinerClass());

		job.setReducerClass(jobInfo.getReducerClass());

		job.setNumReduceTasks(3);
		//job.getConfiguration().set("hdfsPath", hdfsPath.toString());
		// job.getConfiguration().addResource(hdfsPath);
		// System.out.println(job.getConfiguration().get("hdfsPath"));

		return job;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		if (args.length != 2) {
			System.err.println("Error: ");
		}
		System.exit(res);
	}


}
