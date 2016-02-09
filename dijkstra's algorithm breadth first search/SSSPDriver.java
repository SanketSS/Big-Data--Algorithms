package hadoop.assign1;

import hadoop.assign1.Node;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SSSPDriver extends Configured implements Tool {
	/* Enum counter to keep the track of iteration */
	static enum IterationsCounter {
		countOfIterations
	}

	/* Mapper Class */
	public static class MapperSSSP extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Node inNode = new Node(value.toString());

			if (inNode.getnColor() == Node.NodeColor.GRAY) {
				for (String neighbor : inNode.getAdjacentNodes()) {

					Node adjacentNode = new Node();

					adjacentNode.setNodeId(neighbor);
					adjacentNode.setDistance(inNode.getDistance() + 1);
					adjacentNode.setnColor(Node.NodeColor.GRAY);
					adjacentNode.setParentNode(inNode.getParentNode());

					context.write(new Text(adjacentNode.getNodeId()),
							adjacentNode.getNodeInfo());

				}
				inNode.setnColor(Node.NodeColor.BLACK);
			}

			context.write(new Text(inNode.getNodeId()), inNode.getNodeInfo());

		}
	}

	/* Partitioner Class */
	public static class SSSPPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {

			String[] nodeNum = key.toString().split("\t");
			String node = nodeNum[0];
			int nodeInt = Integer.parseInt(node);

			if (numReduceTasks == 0)
				return 0;

			if (nodeInt <= 33) {
				return 0;
			}
			if (nodeInt > 33 && nodeInt <= 66) {

				return 1 % numReduceTasks;
			} else
				return 2 % numReduceTasks;

		}
	}

	/* Reducer */
	public static class ReducerSSSP extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Node outNode = new Node();
			outNode = reduce(key, values, context, outNode);
			if (outNode.getnColor() == Node.NodeColor.GRAY)
				context.getCounter(IterationsCounter.countOfIterations)
						.increment(1L);
		}

		public Node reduce(Text key, Iterable<Text> values, Context context,
				Node outNode) throws IOException, InterruptedException {

			outNode.setNodeId(key.toString());
			for (Text value : values) {

				Node inNode = new Node(key.toString() + "\t" + value.toString());

				if (inNode.getAdjacentNodes().size() > 0) {
					outNode.setAdjacentNodes(inNode.getAdjacentNodes());
				}

				if (inNode.getDistance() < outNode.getDistance()) {
					outNode.setDistance(inNode.getDistance());

					outNode.setParentNode(inNode.getParentNode());
				}

				if (inNode.getnColor().ordinal() > outNode.getnColor()
						.ordinal()) {
					outNode.setnColor(inNode.getnColor());
				}

			}
			context.write(key, new Text(outNode.getNodeInfo()));

			return outNode;

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

	private Job getJobConf(String[] args) throws Exception {

		JobInfo jobInfo = new JobInfo() {
			@Override
			public Class<? extends Reducer> getCombinerClass() {
				return null;
			}

			@Override
			public Class<?> getJarByClass() {
				return SSSPDriver.class;
			}

			@Override
			public Class<? extends Mapper> getMapperClass() {
				return MapperSSSP.class;
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
				return ReducerSSSP.class;
			}

		};
		Job job = setupJob("ssspjob", jobInfo);
		job.setPartitionerClass(SSSPPartitioner.class);
		// return setupJob("ssspjob", jobInfo);
		return job;

	}

	public int run(String[] args) throws Exception {

		int outputCount = 0;

		Job job;

		long exitPointCounter = 1;

		while (exitPointCounter > 0) {

			job = getJobConf(args);
			String input, output;

			if (outputCount == 0)
				input = args[0];
			else
				input = args[1] + outputCount;

			output = args[1] + (outputCount + 1);

			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

			Counters jobCntrs = job.getCounters();
			exitPointCounter = jobCntrs.findCounter(
					IterationsCounter.countOfIterations).getValue();

			outputCount++;

		}

		return 0;
	}

	protected Job setupJob(String jobName, JobInfo jobInfo) throws Exception {

		Job job = new Job(new Configuration(), jobName);
		job.setJarByClass(jobInfo.getJarByClass());

		job.setOutputKeyClass(jobInfo.getOutputKeyClass());
		job.setOutputValueClass(jobInfo.getOutputValueClass());

		job.setMapperClass(jobInfo.getMapperClass());

		if (jobInfo.getCombinerClass() != null)
			job.setCombinerClass(jobInfo.getCombinerClass());

		job.setReducerClass(jobInfo.getReducerClass());

		job.setNumReduceTasks(3);

		return job;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new SSSPDriver(), args);
		if (args.length != 2) {
			System.err.println("Error: ");
		}
		System.exit(res);
	}

}
