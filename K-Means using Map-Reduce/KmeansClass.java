package pkg1;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KmeansClass extends Configured implements Tool {
	
	
	private static final String SPLITTER = "\t";
	private static final String DATA_FILE = "data.txt";
	
	private String  CENTROID_FILE_NAME="centroid.txt";
	private String OUTPUT_FILE_NAME = "/part-r-00000";
	/* Enum counter to keep the track of iteration */
	static enum IterationsCounter {
		countOfIterations
	}

	/* Mapper Class */
	public static class MapperKmeans extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			  Double[] CEN1 = {57.0,70.0,10.0,35.0,87.0,79.0,31.0,81.0,66.0,87.0};//211
			  Double[] CEN2=  {51.0,33.0,40.0,47.0,92.0,94.0,78.0,33.0,47.0,44.0};//638
			  Double[] CEN3= {21.0,66.0,63.0,45.0,28.0,42.0,28.0,12.0,33.0,14.0};//801
			  
			Configuration conf = context.getConfiguration();
			//System.out.println(conf.get("hdfsPath"));
			//URL cacheFiles = conf.getResource(conf.get("hdfsPath"));
			Path ofile = new Path(conf.get("hdfsPath"));
			
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));
			List<Double> centers_next = new ArrayList<Double>();
			String line = br.readLine();
			while (line != null) {
				String[] sp = line.split("\t| ");
				String cen = sp[0];
				
				if(cen.equalsIgnoreCase("c1")){
					String[] dim = sp[1].split(",");
					for (int i = 0; i < dim.length; i++) {
						CEN1[i] = Double.parseDouble(dim[i]);
					}
				}
				if(cen.equalsIgnoreCase("c2")){
					String[] dim = sp[1].split(",");
					for (int i = 0; i < dim.length; i++) {
						CEN2[i] = Double.parseDouble(dim[i]);
					}
				}
				if(cen.equalsIgnoreCase("c3")){
					String[] dim = sp[1].split(",");
					for (int i = 0; i < dim.length; i++) {
						CEN3[i] = Double.parseDouble(dim[i]);
					}
				}
				
				line = br.readLine();
			}
			br.close();
			
			
			String dataLine = value.toString();
			int itr = 0;
			String[] sp1 = dataLine.split("\t");
			double dKey = Double.parseDouble(sp1[0]);
			String[] dim = sp1[1].split(",");
			Double[] dimentions = new Double[10];
			Text nearestMean = null ;
			Double[] meanPoints = new Double[10];

			Double dist1 = 0.0, dist2 = 0.0, dist3 = 0.0;

			for (int i = 0; i < dim.length; i++) {
				dimentions[i] = Double.parseDouble(dim[i]);
			}
			try {
				itr++;
				for (int i = 0; i < 10; i++) {

					dist1 = dist1 + (Math.pow((dimentions[i] - CEN1[i]), 2));
					dist2 = dist2 + (Math.pow((dimentions[i] - CEN2[i]), 2));
					dist3 = dist3 +(Math.pow((dimentions[i] - CEN3[i]), 2));

				}

			} catch (NullPointerException e) {
				System.out.println("Dim" + "=======>>>" + dimentions.toString()
						+ "+++++++" + dataLine + "....." + itr);
			}

			dist1 = Math.sqrt(dist1);
			dist2 = Math.sqrt(dist2);
			dist3 = Math.sqrt(dist3);

			if (dist1 < dist2 && dist1 < dist3) {
				nearestMean = new Text("C1");

			}

			if (dist2 < dist1 && dist2 < dist3) {
				nearestMean = new Text("C2");
			}
			if (dist3 < dist1 && dist3 < dist2) {
				nearestMean = new Text("C3");
			}
			/*DataPointInfo dInfo = new DataPointInfo();
			dInfo.setdPoint(dKey);
			dInfo.setDataDimArray(dimentions);*/
			
		
			context.write(nearestMean ,new Text(sp1[1]));

		}
		
		
	}

		/* Partitioner Class */
		public static class PartitionerKmeans extends Partitioner<Text, Text> {

			@Override
			public int getPartition(Text key, Text value, int numReduceTasks) {

				String centroid = key.toString();


				if (numReduceTasks == 0)
					return 0;

				if (centroid.equalsIgnoreCase("c1")) {
					return 0;
				}
				if (centroid.equalsIgnoreCase("c2")){

					return 1 % numReduceTasks;
				} else
					return 2 % numReduceTasks;

			}
		}

		/* Reducer */
		public static class ReducerKmeans extends
				Reducer<Text, Text, Text, Text> {

			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
				int totalPts1=0,totalPts2=0,totalPts3=0;
				String[] newCen1=new String[10],newCen2 =new String[10], newCen3=new String[10];
				
			if (key.toString().equalsIgnoreCase("c1")) {
				Double[] dimCen1={0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
				for (Text value : values) {
					totalPts1++;
					String[] valueStr = value.toString().split(",");

					for (int i = 0; i < valueStr.length; i++) {
						dimCen1[i] = dimCen1[i] + Double.parseDouble(valueStr[i]);
					}

				}
				for(int i=0;i<10;i++){	
					newCen1[i] = String.valueOf((dimCen1[i]/totalPts1));
				}
				StringBuilder builder = new StringBuilder();
				for(String s : newCen1) {
				    builder.append(s).append(",");
				}
				
				context.write(key, new Text( builder.toString()));
			}
			if (key.toString().equalsIgnoreCase("c2")) {
				Double[] dimCen2={0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
				for (Text value : values) {
					totalPts2++;
					String[] valueStr = value.toString().split(",");
					
					for (int i = 0; i < valueStr.length; i++) {
						dimCen2[i] = dimCen2[i] + Double.parseDouble(valueStr[i]);
					}

				}
				
				for(int i=0;i<10;i++){	
					newCen2[i] = String.valueOf((dimCen2[i]/totalPts2));
				}
				StringBuilder builder = new StringBuilder();
				for(String s : newCen2) {
				    builder.append(s).append(",");
				}
				
				context.write(key, new Text( builder.toString()));
			}
			
			if (key.toString().equalsIgnoreCase("c3")) {
				Double[] dimCen3={0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
				for (Text value : values) {
					totalPts3++;
					String[] valueStr = value.toString().split(",");

					for (int i = 0; i < valueStr.length; i++) {
						dimCen3[i] = dimCen3[i] + Double.parseDouble(valueStr[i]);
					}

				}
				
				for(int i=0;i<10;i++){	
					newCen3[i] = String.valueOf((dimCen3[i]/totalPts3));
				}
				StringBuilder builder = new StringBuilder();
				for(String s : newCen3) {
				    builder.append(s).append(",");
				}
				
				context.write(key, new Text( builder.toString()));
			}
			
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
					return KmeansClass.class;
				}

				@Override
				public Class<? extends Mapper> getMapperClass() {
					return MapperKmeans.class;
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
					return ReducerKmeans.class;
				}

			};
			Job job = setupJob("Kmeans", jobInfo,hdfsPath);
			job.setPartitionerClass(PartitionerKmeans.class);
			// return setupJob("K", jobInfo);
			return job;

		}

		public int run(String[] args) throws Exception {

			int outputCount = 0;
			int nextInputCount=0;
			Job job;
			long exitPointCounter = 1;
			boolean isdone;
			int iteration = 0;
			
			Double[] oldC1 = new Double[10];
			Double[] oldC2 = new Double[10];
			Double[] oldC3 = new Double[10];
			
			Double[] newC1 = new Double[10];
			Double[] newC2 = new Double[10];
			Double[] newC3 = new Double[10];
			
			
			while (exitPointCounter > 0) {

				
				String input, output, again_input = null;

				
				input = args[0];
				output = args[1] + (outputCount + 1);
				
				if (outputCount == 0)
					again_input = args[1];
				else
					again_input = args[1] + nextInputCount;
				
				
				Path hdfsPath;

				String prev;
				if (iteration == 0) {
					prev = input + CENTROID_FILE_NAME;
					hdfsPath =new Path(prev);
					//DistributedCache.addCacheFile(hdfsPath.toUri(), job.getConfiguration());
					
				} else {
					prev = again_input+ OUTPUT_FILE_NAME;
					 hdfsPath =new Path(prev);
					//DistributedCache.addCacheFile(hdfsPath.toUri(), job.getConfiguration());
				}
				job = getJobConf(args, hdfsPath);
				
				FileInputFormat.setInputPaths(job, new Path(input+DATA_FILE));
				FileOutputFormat.setOutputPath(job, new Path(output));
				
				job.waitForCompletion(true);

				/*Counters jobCntrs = job.getCounters();
				exitPointCounter = jobCntrs.findCounter(
						IterationsCounter.countOfIterations).getValue();
				outputCount++;*/
				
				Path ofile = new Path(output+OUTPUT_FILE_NAME);
				
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(ofile)));
				String line = br.readLine();
				while (line != null) {
					String[] sp = line.split("\t| ");
					String cen = sp[0];
					
					if(cen.equalsIgnoreCase("c1")){
						String[] dim = sp[1].split(",");
						for (int i = 0; i < dim.length; i++) {
							newC1[i] = Double.parseDouble(dim[i]);
						}
					}
					if(cen.equalsIgnoreCase("c2")){
						String[] dim = sp[1].split(",");
						for (int i = 0; i < dim.length; i++) {
							newC2[i] = Double.parseDouble(dim[i]);
						}
					}
					if(cen.equalsIgnoreCase("c3")){
						String[] dim = sp[1].split(",");
						for (int i = 0; i < dim.length; i++) {
							newC3[i] = Double.parseDouble(dim[i]);
						}
					}
					
					line = br.readLine();
				}
				br.close();
				
				
				
				Path prevfile = new Path(prev);
				FileSystem fs1 = FileSystem.get(new Configuration());
				BufferedReader br1 = new BufferedReader(new InputStreamReader(
						fs1.open(prevfile)));
				String l = br1.readLine();
				while (l != null) {
					String[] sp = l.split("\t| ");
					String cen = sp[0];
					
					if(cen.equalsIgnoreCase("c1")){
						String[] dim = sp[1].split(",");
						for (int i = 0; i < dim.length; i++) {
							oldC1[i] = Double.parseDouble(dim[i]);
						}
					}
					if(cen.equalsIgnoreCase("c2")){
						String[] dim = sp[1].split(",");
						for (int i = 0; i < dim.length; i++) {
							oldC2[i] = Double.parseDouble(dim[i]);
						}
					}
					if(cen.equalsIgnoreCase("c3")){
						String[] dim = sp[1].split(",");
						for (int i = 0; i < dim.length; i++) {
							oldC3[i] = Double.parseDouble(dim[i]);
						}
					}
					
					l = br1.readLine();
				}
				br1.close();
				// Sort the old centroid and new centroid and check for convergence
				// condition
				Double dist1 = 0.0, dist2 = 0.0, dist3 = 0.0;
				
				for (int i = 0; i < 10; i++) {

					dist1 = dist1 + ((oldC1[i] - newC1[i])
							* (oldC1[i] - newC1[i]));
					dist2 = dist2 + ((oldC2[i] - newC2[i])
							* (oldC2[i] - newC2[i]));
					dist3 = dist3 + ((oldC3[i] - newC3[i])
							* (oldC3[i] - newC3[i]));
				}
				
				dist1 = Math.sqrt(dist1);
				dist2 = Math.sqrt(dist2);
				dist3 = Math.sqrt(dist3);

				if(dist1<.1){
					if(dist2<.1){
						if(dist3<.1)
						{
							exitPointCounter =0;
						}
					}
				}
				
				
				++iteration;
				outputCount++;
				nextInputCount++;
				
				//output = args[1] + System.nanoTime();
			}
			

			return 0;
		}

		protected Job setupJob(String jobName, JobInfo jobInfo,Path hdfsPath)
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
			job.getConfiguration().set("hdfsPath", hdfsPath.toString());
			//job.getConfiguration().addResource(hdfsPath);
			//System.out.println(job.getConfiguration().get("hdfsPath"));

			return job;
		}

		public static void main(String[] args) throws Exception {

			int res = ToolRunner.run(new Configuration(), new KmeansClass(),
					args);
			if (args.length != 2) {
				System.err.println("Error: ");
			}
			System.exit(res);
		}

	
}