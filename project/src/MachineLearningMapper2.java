import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MachineLearningMapper2 extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String readLine = value.toString();
		String[] splitLine=readLine.split("\t");
		String district=splitLine[0];
		String date=splitLine[1];
		String crime=splitLine[2];
		context.write(new Text(district+"\t"+date+"\t"+crime),new Text("one"));
	}
		
		
}

