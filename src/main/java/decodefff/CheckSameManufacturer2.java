package decodefff;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FieldWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FieldInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FieldOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import supplyframe.utils.FieldIntPair;


/*
hadoop jar decodefff-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
decodefff.CheckSameManufacturer2 \
-Dmapred.reduce.tasks=10 -Ddfs.replication=1 \
/user/amishra/partsio_extract/ \
/user/amishra/fff_pairwise_man1  \
/user/amishra/fff_pairwise_man2
 */
public class CheckSameManufacturer2 extends Configured implements Tool {

	
	public static void main(String[] args) throws Exception {
		int rc=ToolRunner.run(new Configuration(), new CheckSameManufacturer2(), args);
		System.exit(rc);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec",	"org.apache.hadoop.io.compress.SnappyCodec");
		
	    conf.set("mapred.output.compress", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
		
		Job job=new Job(conf);
		job.setJarByClass(CheckSameManufacturer2.class);
		job.setJobName("CheckSameManufacturer2");
		
		
		job.setMapOutputKeyClass(FieldIntPair.class);
		job.setMapOutputValueClass(FieldIntPair.class);
		job.setOutputKeyClass(FieldWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
	    job.setGroupingComparatorClass(FieldIntPair.GroupComparator.class);
	    job.setPartitionerClass(FieldIntPair.KeyPartitioner.class);
		
		job.setOutputFormatClass(FieldOutputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), FieldInputFormat.class, ExtractedPartsIOMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FFFPartsMapper.class);
		job.setReducerClass(FFFPartReducer2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true) ? 0:1;
	}
	
	
	public static class FFFPartReducer2 extends Reducer<FieldIntPair, FieldIntPair, FieldWritable, NullWritable>{
		private FieldWritable keyOut =new FieldWritable(
				"part1" + "\t" + "part2" + "\t" + "manu1" + "\t" + "manu2"
				);
		
		@Override
		protected void reduce(FieldIntPair key, Iterable<FieldIntPair> vals,
				Reducer<FieldIntPair, FieldIntPair, FieldWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
				
			context.progress();
			String manu="#####";
			Iterator<FieldIntPair> itr=vals.iterator();
			while(itr.hasNext()){
				FieldIntPair fip=itr.next();
				if(fip.mark.get()==0){
					manu = fip.field.get("manufacturer");
					
				}else{
					keyOut.set(fip.field.toString()+"\t"+manu);
					context.write(keyOut, NullWritable.get());
				}
				
			}
		}
		
	}
	
	
	public static class FFFPartsMapper extends Mapper<LongWritable , Text, FieldIntPair , FieldIntPair >{
		private FieldIntPair keyOut = new FieldIntPair(new FieldWritable("join_part"), 1);
		private FieldIntPair valOut = new FieldIntPair(new FieldWritable(
					"part1" + "\t" + "part2" + "\t" + "manu1"
		
					),																	1);
		
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FieldIntPair, FieldIntPair>.Context context)
				throws IOException, InterruptedException {
			
			String[] toks=value.toString().split("\t");

			keyOut.field.set(toks[1]);
			valOut.field.set(value);
			context.write(keyOut, valOut);
		}
		
		
	}
	
	
	
	
	public static class ExtractedPartsIOMapper extends Mapper<LongWritable, FieldWritable, FieldIntPair, FieldIntPair>{
		private FieldIntPair keyOut = new FieldIntPair(new FieldWritable("join_part"), 0);
		private FieldIntPair valOut = new FieldIntPair(new FieldWritable("manufacturer") , 0); 
		
		@Override
		protected void map(LongWritable key, FieldWritable value,
				Mapper<LongWritable, FieldWritable, FieldIntPair, FieldIntPair>.Context context)
				throws IOException, InterruptedException {

			String p=value.get("part_number");
			String m=value.get("manufacturer");
			
			keyOut.field.set(p);
			valOut.field.set(m);
			context.write(keyOut, valOut);
			
		}
	}

}
