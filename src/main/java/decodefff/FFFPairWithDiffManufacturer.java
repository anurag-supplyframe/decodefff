package decodefff;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FieldInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
hadoop jar decodefff-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
decodefff.FFFPairWithDiffManufacturer \
-Dmapred.reduce.tasks=0 \
/user/amishra/fff_pairwise_man2
 */
public class FFFPairWithDiffManufacturer extends Configured implements Tool {

	
	public static void main(String[] args) throws Exception {
		int rc= ToolRunner.run(new Configuration(), new FFFPairWithDiffManufacturer(), args);
		System.exit(rc);
	}
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf= getConf();
		
		conf.set("mapred.reduce.tasks", "0");

		
		Job job = new Job(conf);
		job.setJarByClass(FFFPairWithDiffManufacturer.class);
		job.setJobName("FFFPairWithDiffManufacturer");
		job.setMapperClass(DiffManufacturerMapper.class);
		

		
		
		job.setInputFormatClass(FieldInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);//directs to /dev/null
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		
		
		int rc= job.waitForCompletion(true) ? 0 : 1;
		
		
		/*
		 * Write the result to a file
		 */
		long cnt=job.getCounters().findCounter("DiffManufacturerMapper", "are.different").getValue();
		long recProcessed=job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream dos=fs.create(new Path("/user/amishra/fff_pairwise_diff_manu_count.txt"), true);
		PrintStream ps=new PrintStream(dos);
		ps.println("The number of fff parts which have different manufacturers are:" + cnt);
		ps.println("The number of records processed are:" + recProcessed);
		ps.print(((double)cnt/recProcessed) * 100);
		
		ps.flush();
		ps.close();
		
		
		
		
		return rc;
	}
	
	
	
	public static class DiffManufacturerMapper extends Mapper<LongWritable, FieldWritable, NullWritable, NullWritable>{
		
		
		@Override
		protected void map(LongWritable key, FieldWritable value,
				Mapper<LongWritable, FieldWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			
			String manu1=value.get("manu1");
			String manu2=value.get("manu2");
			if(
					!"#####".equals(manu1) &&
					!"#####".equals(manu2) &&
					! manu1.equals(manu2)
					){
				context.getCounter("DiffManufacturerMapper", "are.different").increment(1);
			}
			
		}
	}

}
