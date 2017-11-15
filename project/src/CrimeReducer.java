import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimeReducer extends Reducer<Text,Text,Text,Text>{
	//private static float maximum = 0;
	public void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			String[] valsplit = val.toString().split(",");

//			context.write(new Text(String.valueOf(valsplit.length)), new Text(""));
			String date=valsplit[0];
			String block=valsplit[1];
			String illUniCriRepCode=valsplit[2];
			String primaryDescription=valsplit[3];
			String secondaryDescription=valsplit[4];
			String locationDescription=valsplit[5];
			String arrest=valsplit[6];
			String year=valsplit[7];
			context.write(new Text(date+","+key+","+block+","+illUniCriRepCode+","+primaryDescription+","+secondaryDescription+","+locationDescription+","+arrest+","+year), new Text(""));
		
		}	
	}
}
