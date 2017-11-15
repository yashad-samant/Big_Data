import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeMapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String readLine = value.toString();
		String[] splitLine=readLine.split(",");
		String caseNumber=splitLine[1];
		String date=splitLine[2];
		String block=splitLine[3];
		String illUniCriRepCode=splitLine[4];
		String primaryDescription=splitLine[5];
		String secondaryDescription=splitLine[6];
		String locationDescription=splitLine[7];
		String arrest=splitLine[8];
		String year=splitLine[17];
		context.write(new Text(caseNumber),new Text(date+","+block+","+illUniCriRepCode+","+primaryDescription+","+secondaryDescription+","+locationDescription+","+arrest+","+year));
	}
		
		
}

