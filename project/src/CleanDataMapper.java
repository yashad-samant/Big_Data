import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanDataMapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String readLine = value.toString();
		String [] splitLine=readLine.split(",");
		String[] newValues=new String[25];
		if(splitLine.length<25){
				for(int i=0;i <splitLine.length;i++){
					if(!(splitLine[i].equals("")||splitLine[i].equals(null)||splitLine[i].equals("null"))){
						newValues[i]=splitLine[i];
					}
					else{
						newValues[i]= "EmptyValue";
					}
				}
				for(int y=splitLine.length;y<newValues.length;y++){
					newValues[y]= "EmptyValue";
				}
				String newDataLine = "";
				for(int i=0;i <newValues.length;i++){
					if(i!=newValues.length-1){
						newDataLine +=newValues[i]+",";
						//System.out.print(newValues[i]+",");
					}
					else{
						newDataLine +=newValues[i];
						//System.out.print(newValues[i]);
					}
					
				}
				context.write(new Text(newDataLine),new Text(""));
				//System.out.println(newDataLine);
				//System.out.println("");
					
		}
		else if(splitLine.length==25){
			for(int i=0;i <splitLine.length;i++){
				if(!(splitLine[i].equals("")||splitLine[i].equals("null"))){
					newValues[i]=splitLine[i];
				}
				else{
					newValues[i]= "EmptyValue";
				}
			}
			String newDataLine = "";
			for(int i=0;i <newValues.length;i++){
				if(i!=newValues.length-1){
					newDataLine +=newValues[i]+",";
					//System.out.print(newValues[i]+",");
				}
				
				else{
					newDataLine +=newValues[i];
					//System.out.print(newValues[i]);
				}
			}
			context.write(new Text(newDataLine),new Text(""));
		}
	}
		
		
}

