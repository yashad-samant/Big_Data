import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class MainClass {
final static String output1="cleanData";
final static String output2="dataExtraction";
final static String output3="crimeCountBasedCategory";
final static String output4="dataBasedOnDistrict";
final static String output5="machineLearning";	
final static String output6="machineLearning2";	
final static String output7="monthsMapper";	
public static void main(String[] args) throws IOException, ClassNotFoundException,
InterruptedException {
if (args.length != 2) {
 System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
 System.exit(-1);
}

Configuration conf =new Configuration();
Job job=Job.getInstance(conf);

//job=Job.getInstance(conf);
//job.setJarByClass(MainClass.class);
//job.setMapperClass(CleanDataMapper.class);
//job.setReducerClass(CleanDataReducer.class);
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(Text.class);
//job.setInputFormatClass(TextInputFormat.class);
//job.setOutputFormatClass(TextOutputFormat.class);
//FileInputFormat.setInputPaths(job, new Path(args[0]));
//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+output1));
//if(job.waitForCompletion(true)){
//	System.out.println("Data cleaned");
//}
//
//
//job=Job.getInstance(conf);
//job.setJarByClass(MainClass.class);
//job.setMapperClass(DataExtractionMapper.class);
//job.setReducerClass(DataExtractionReducer.class);
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(Text.class);
//job.setInputFormatClass(TextInputFormat.class);
//job.setOutputFormatClass(TextOutputFormat.class);
//FileInputFormat.setInputPaths(job, new Path(args[1]+"/"+output1+"/part-r-00000"));
//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+output2));
//if(job.waitForCompletion(true)){
//	System.out.println("Data Extraction done");
//}
//
//
//job=Job.getInstance(conf);
//job.setJarByClass(MainClass.class);
//job.setMapperClass(CategoryMapper.class);
//job.setReducerClass(CategoryReducer.class);
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(Text.class);
//job.setInputFormatClass(TextInputFormat.class);
//job.setOutputFormatClass(TextOutputFormat.class);
//FileInputFormat.setInputPaths(job, new Path(args[1]+"/"+output1+"/part-r-00000"));
//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+output3));
//if(job.waitForCompletion(true)){
//	System.out.println("Crime count Based on category done");
//}
//
//job=Job.getInstance(conf);
//job.setJarByClass(MainClass.class);
//job.setMapperClass(DistrictMapper.class);
//job.setReducerClass(DistrictReducer.class);
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(Text.class);
//job.setInputFormatClass(TextInputFormat.class);
//job.setOutputFormatClass(TextOutputFormat.class);
//FileInputFormat.setInputPaths(job, new Path(args[1]+"/"+output2+"/part-r-00000"));
//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+output4));
//if(job.waitForCompletion(true)){
//	System.out.println("Data sort Based on District");
//}
//job=Job.getInstance(conf);
//job.setJarByClass(MainClass.class);
//job.setMapperClass(MachineLearningMapper.class);
//job.setReducerClass(MachineLearningReducer.class);
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(Text.class);
//job.setInputFormatClass(TextInputFormat.class);
//job.setOutputFormatClass(TextOutputFormat.class);
//FileInputFormat.setInputPaths(job, new Path(args[1]+"/"+output2+"/part-r-00000"));
//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+output5));
//if(job.waitForCompletion(true)){
//	System.out.println("Data For Machine Learning1");
//}

job=Job.getInstance(conf);
job.setJarByClass(MainClass.class);
job.setMapperClass(MachineLearningMapper2.class);
job.setReducerClass(MachineLearningReducer2.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.setInputPaths(job, new Path(args[1]+"/"+output5+"/part-r-00000"));
FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+output6));
if(job.waitForCompletion(true)){
	System.out.println("Data For Machine Learning2");
}


job=Job.getInstance(conf);
job.setJarByClass(MainClass.class);
job.setMapperClass(MonthYearMapper.class);
job.setReducerClass(MonthYearReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.setInputPaths(job, new Path(args[1]+"/"+output6+"/part-r-00000"));
FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+output7));
if(job.waitForCompletion(true)){
	System.out.println("Months Year Details Got");
}






//job=Job.getInstance(conf);
//job.setJarByClass(MainClass.class);
//job.setMapperClass(MachineLearningMapper2.class);
//job.setReducerClass(MachineLearningReducer2.class);
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(Text.class);
//job.setInputFormatClass(TextInputFormat.class);
//job.setOutputFormatClass(TextOutputFormat.class);
//FileInputFormat.setInputPaths(job, new Path(args[1]+"/"+output6+"/part-r-00000"));
//FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+output7));
//if(job.waitForCompletion(true)){
//	System.out.println("Data For Machine Learning3");
//}



System.exit(job.waitForCompletion(true) ? 0 : 1);



//job.setJarByClass(MainClass.class);
//job.setMapperClass(CleanDataMapper.class);
//job.setReducerClass(CleanDataReducer.class);
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(Text.class);
//job.setInputFormatClass(TextInputFormat.class);
//job.setOutputFormatClass(TextOutputFormat.class);
//FileInputFormat.setInputPaths(job, new Path(args[0]));
//FileOutputFormat.setOutputPath(job, new Path(args[1]));
//System.exit(job.waitForCompletion(true) ? 0 : 1);




////link to the number of authors file
//Path path= new Path("hdfs://boise30182"+args[1]+"/numberOfAuthor");
//FileSystem fs=FileSystem.get(new Configuration());
//BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
//int authorCount=0;
//String line;
//while((line=br.readLine()) !=null){
//	String[] linesplit=line.split("\t");
//	authorCount=Integer.valueOf(linesplit[1]);
//}
//br.close();



}
}