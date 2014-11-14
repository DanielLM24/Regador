import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PumpMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//Toma del archivo de texto la fecha y la bomba correspondiente.
		String line = value.toString();
		String date = line.substring(0,8);
		int bomba = Integer.parseInt(line.substring(16,18));
		if(bomba != 0 && line.charAt(39)=='9')
			context.write(new Text(date), new IntWritable(bomba));
	}
}