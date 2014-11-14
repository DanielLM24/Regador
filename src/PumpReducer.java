import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PumpReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context)
					throws IOException, InterruptedException {
		int contador = 0;
		int contador_2 = 0;
		int contador_3 = 0;
		
		//Encuentra la bomba que se activo.
		for (IntWritable value : values) {
			if(value.equals(new IntWritable(1)))
				contador++;
			if(value.equals(new IntWritable(2)))
				contador_2++;
			if(value.equals(new IntWritable(3)))
				contador_3++;
		}
		int bomba = 0;
		//Determina que bomba se activo mas veces.
		int maxValue = Integer.MIN_VALUE;
		if(maxValue<contador){
			maxValue=contador;
			bomba = 1;
		}
		if(maxValue<contador_2){
			maxValue=contador_2;
			bomba = 2;
		}
		if(maxValue<contador_3){
			maxValue=contador_3;
			bomba = 3;
		}
			
		context.write(key, new IntWritable(bomba));
	}
}