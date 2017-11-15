import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistrictReducer extends Reducer<Text,Text,Text,Text>{
	//private static float maximum = 0;
	public void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (Text val : values) {
			count++;
			String[] valsplit = val.toString().split(",");
			String crimeType=valsplit[0];
			String crimePlace=valsplit[1];
			//String date=valsplit[2];
			context.write(new Text(key), new Text(crimeType+","+crimePlace+","+count));
		}	
	}
}
