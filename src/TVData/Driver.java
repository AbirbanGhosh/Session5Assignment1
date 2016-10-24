package TVData;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;




public class Driver {
	
	public static void main(String [] args) throws Exception{
		
		if(args.length != 3)
			throw new Exception("Invalid arguments. usage <InputFile>"
					+ "<OutputPat> <TaskType: 4/5>");
		if(!args[2].equals("4") && !args[2].equals("5"))
			throw new Exception("Task type should be either 4 or 5");
		//create the configuration object
		Configuration conf = new Configuration();
				
		Job job = Job.getInstance(conf);
		job.setJobName("Total Unit Sold per Company");
		//set the proper task name
				
		job.setJarByClass(Driver.class);
		
		//set class for map task
		job.setMapperClass(MapTask.class);
		
		
		
		
		//set class for reduce task
		job.setReducerClass(ReduceTask.class);
		if(args[2].equals("4")){
			//set partitioner class
			job.setPartitionerClass(PartitionerTask.class);
			//set reducer task count
			job.setNumReduceTasks(4);
		}
		else{
			//set combiner class
			job.setCombinerClass(ReduceTask.class);
			//set reducer task count
			job.setNumReduceTasks(1);
		}
		//set output key and value for map task
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//set output key and value for reducer Task
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//set input and output formatter class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		
		//set input and out files
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//wait until it completes all the task
		job.waitForCompletion(true);
	}

}
