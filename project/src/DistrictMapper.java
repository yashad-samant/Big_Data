import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistrictMapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String readLine = value.toString();
		String[] splitLine=readLine.split(",");
		String date=splitLine[1];
		String [] datesplit = date.split("/");
		String month = datesplit[0];
		String crimeType=splitLine[2];
		String crimePlace=splitLine[3];
		String district=Integer.valueOf(splitLine[5]).toString();
		context.write(new Text(district+" "+month),new Text(crimeType+","+crimePlace));
	}
		
		
}

