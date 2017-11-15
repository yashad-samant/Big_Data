import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataExtractionReducer extends Reducer<Text,Text,Text,Text>{
	//private static float maximum = 0;
	public void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			String[] valsplit = val.toString().split(",");
			String caseNumber=valsplit[0];
			String primaryDescription=valsplit[1];
			String locationDescription=valsplit[2];
			String arrest=valsplit[3];
			String communityArea=valsplit[4];
			String updatedOn=valsplit[5];
			context.write(new Text(key+","+caseNumber+","+primaryDescription+","+locationDescription+","+arrest+","+communityArea+","+updatedOn), new Text(""));
		}	
	}
}
