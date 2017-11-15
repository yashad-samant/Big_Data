import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataExtractionMapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String readLine = value.toString();
		String[] splitLine=readLine.split(",");	
		String caseNumber=splitLine[1];
		String date=splitLine[2];
		String[] dateExtract=date.split(" ");
		//split date for getting time proper
		int timeSplitMain=Integer.valueOf(dateExtract[1].split(":")[0]);
		//night 9pm-11pm 12am-5am
		if((((timeSplitMain >= 1 && timeSplitMain <=5)||timeSplitMain==12) && dateExtract[2].equals("AM"))||(((timeSplitMain >= 9 && timeSplitMain <=11))&& dateExtract[2].equals("PM"))){
			date=dateExtract[0]+" "+"4";
			
		}
		//morning 6am -11am
		else if(timeSplitMain >= 6 && timeSplitMain <=11 && dateExtract[2].equals("AM")){
			date=dateExtract[0]+" "+"1";
		}
		//afternoon 12pm-3pm
		else if(((timeSplitMain >= 1 && timeSplitMain <=3)||timeSplitMain==12) && dateExtract[2].equals("PM")){
			date=dateExtract[0]+" "+"2";
		}
		//evening 4pm -8pm
		
		else if(timeSplitMain >= 4 && timeSplitMain <=8 && dateExtract[2].equals("PM")){
			date=dateExtract[0]+" "+"3";
		}
		
		//String block=splitLine[3];
		//String illUniCriRepCode=splitLine[4];
		String primaryDescription=splitLine[5];
		//String secondaryDescription=splitLine[6];	
		String locationDescription=splitLine[7];
		String arrest=splitLine[8];
		String communityArea=splitLine[13];
		String updatedOn=splitLine[18];
		
		context.write(new Text(caseNumber),new Text(date+","+primaryDescription+","+locationDescription+","+arrest+","+communityArea+","+updatedOn));
	}
		
		
}

