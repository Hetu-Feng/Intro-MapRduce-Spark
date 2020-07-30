import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class AvgExp {

  public static class AvgExpMapper
        extends Mapper<Object, Text, Text, FloatWritable>{
      
    public void map(Object offset, Text row, Context context
                    ) throws IOException, InterruptedException {
      
      String[] columns = row.toString().split(",");
      String[] messedRow = {"'COD'","'FSM'","'VGB'","'VIR'"};
      boolean fckmess = Arrays.asList(messedRow).contains(columns[0]);
      
      if (fckmess){

        Float gnp = Float.parseFloat(columns[9].substring(2, columns[9].length()-1));
        if(gnp > 10000){
          String contnt = columns[3];
          Float liEx = Float.parseFloat(columns[8].substring(2,columns[8].length()-1));
          context.write(new Text(contnt), new FloatWritable(liEx));
        }
      }

      else{
        
        Float gnp = Float.parseFloat(columns[8].substring(2, columns[8].length()-1));
        if(gnp > 10000){
          String contnt = columns[2];
          Float liEx = Float.parseFloat(columns[7].substring(2,columns[7].length()-1));
          context.write(new Text(contnt), new FloatWritable(liEx));
        }
      }
    }
  }
  
  public static class AvgExpReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    
    private FloatWritable result = new FloatWritable();

    public void reduce(Text continent, Iterable<FloatWritable> lifeExpectancy,
                        Context context
                        ) throws IOException, InterruptedException {
      float leTotal =0.0f;
      int count = 0;
      
      for (FloatWritable val : lifeExpectancy) {
        leTotal += val.get();
        count++;
      }
      result.set(leTotal/count);

      if (count>=5){
        context.write(continent, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: AvgExp <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "hw5");
    job.setJarByClass(AvgExp.class);
    job.setMapperClass(AvgExpMapper.class);
    job.setReducerClass(AvgExpReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}




