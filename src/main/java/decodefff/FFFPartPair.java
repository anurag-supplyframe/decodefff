package decodefff;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/*
hadoop jar decodefff-0.0.1-SNAPSHOT-jar-with-dependencies.jar decodefff.FFFPartPair \
-Ddfs.replication=1 \
/prod/partsio/fff/ /user/amishra/fff_pairwise 
 */

public class FFFPartPair extends Configured implements Tool{

	
	public static void main(String[] args)  throws Exception {
		int ret = ToolRunner.run(new Configuration(), new FFFPartPair(), args);
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf= getConf();
		
	    conf.set("mapred.output.compress", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
	    conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec",	"org.apache.hadoop.io.compress.GzipCodec");
		
		conf.set("mapred.reduce.tasks", "0");
		
		
		Job job = new Job(conf);
		job.setJarByClass(FFFPartPair.class);
		job.setJobName("FFFPartPair");
		job.setMapperClass(FpMapper.class);
		

		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : 1;
		
		

	}
	
	
	public static class FpMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] catAndPartStr = value.toString().split("\\s+");
			String grp=catAndPartStr[0];
			String[] parts = catAndPartStr[1].split("\u0007");
			
			HashSet<String> filteredPartsSet = new HashSet<String>(parts.length/2);
			
			for(int i=0 ; i < parts.length ; i += 2 ){
				filteredPartsSet.add(parts[i+1].replaceAll("[^a-zA-Z0-9]", "").toUpperCase());
				//remove all non-alphanumeric characters and convert to upper case
				
			}
			
			String[] filteredParts = new String[filteredPartsSet.size()];
			filteredPartsSet.toArray(filteredParts);
			
			
			
			if(filteredParts.length >=500){
				context.setStatus("Processing big:"+filteredParts.length);
			}
			for(int i = 0 ; i< filteredParts.length ; i++){
				for(int j=0 ; j< filteredParts.length ; j++){
					if(i!=j){

						context.write(new Text(filteredParts[i] + "\t" + filteredParts[j])   ,  NullWritable.get());
					}
				}
				
			}
			
			context.progress();
			
		}
	}
	
	

}
