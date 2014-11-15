/* 
 * Universidad del Valle de Guatemala
 * Algoritmos y Estructura de Datos
 * Peter Bennett 13243
 * Daniel Lara Moir 13424
 * Santiago Gonzalez 13263
 * 
 * PumpDriver.java
 * 
 * Programa que mapea y reduce un conjunto de datos generado por un regador automático.
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This class is responsible for running map reduce job*/
public class PumpDriver extends Configured implements Tool{
	public int run(String[] args) throws Exception
	{
		if(args.length !=2) {
			System.err.println("Usage: PumpDriver <input path> <outputpath>");
			System.exit(-1);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(PumpDriver.class);
		job.setJobName("Max Pump");

		//Abre el archivo de texto y almacena el resultado en C:\
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		job.setMapperClass(PumpMapper.class);
		job.setReducerClass(PumpReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0:1); 
		//Corre el trabajo de Map-Reduce.
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		PumpDriver driver = new PumpDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}