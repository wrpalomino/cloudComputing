package org.myorg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;


/**
 * Class to make the count of specific words from a set of given files
 * 
 * @author wrpalomino
 * @since 2016-06-29
 */
public class WordCount 
{

  /**
   * Implements the count of words and grouped by State name (taken form the input files names)
   */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    protected String[] terms = {"education", "politics", "sports", "agriculture"};  // specific word to find

    /**
     * Counts the words by composite key "state:word"
     * 
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String tmp_token;
      
      FileSplit fsFileSplit = (FileSplit)context.getInputSplit();
      String filename = fsFileSplit.getPath().getName();
      
      while (itr.hasMoreTokens()) {
        tmp_token = itr.nextToken().toLowerCase();                              
        if (Arrays.asList(terms).contains(tmp_token)) { // only save if the word matches
          word.set(filename+":"+tmp_token);
          context.write(word, one);           
        }
      }
    }
  }

  
  /**
   * Implements the aggregation of words by the composite key
   */
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    /**
     * Aggregates the data (sum)
     * 
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  
  /**
   * Implements the second map function to rebuild the pairs from the first reduce function 
   */
  public static class TokenizerMapper2
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text word2 = new Text();

    /**
     * Rebuilds the pairs from the first reduce function
     * 
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {      
      String[] parts = value.toString().split("\\t");
      String[] parts2 = parts[0].split(":");
           
      word.set(parts2[1]);
      word2.set(parts2[0]+":"+parts[1]);
      context.write(word, word2);
    }
  }
  
  
  /**
   * Implements the ranking of the top 3 states (input files) where the specific words are used the most 
   */
  public static class IntSumReducer2
       extends Reducer<Text,Text,Text,Text> {
    
    /**
     * Sort and select the top 3 pairs (word state:count)
     * 
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {                              
      String[] parts;
      Map<String, Integer> countMap = new HashMap<String, Integer>();
      for (Text val : values) {
        parts = val.toString().split(":");
        countMap.put(parts[0], Integer.parseInt(parts[1]));        
      }
      Map<String, Integer> sortedMap = sortByComparator(countMap);
      
      int counter = 0;
      Text value = new Text();
      for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
        if (counter ++ == 3) {
          break;
        }
        value.set(entry.getKey()+":"+String.valueOf(entry.getValue()));
        context.write(key, value);
      }                        
    }
    
    /**
     * Sorts the Map structure by value (descending)
     * 
     * @param unsortMap
     * @return 
     */
    private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap)
    {
      List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(unsortMap.entrySet()); // Convert Map to List
      
      Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {     // Sort list with comparator, to compare the Map values
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
          return (o2.getValue()).compareTo(o1.getValue());
        }
      });
      
      Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();    // Convert sorted map back to a Map
      for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it.hasNext();) {
        Map.Entry<String, Integer> entry = it.next();
        sortedMap.put(entry.getKey(), entry.getValue());
      }
      return sortedMap;
    }

  }
  
  
  /**
   * Main Function
   * 
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    
    Job job = Job.getInstance(conf, "word count base");     // first job: count words and group by state
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
    Job job2 = Job.getInstance(conf, "word rank");          // second job: get the top 3 ranking
    job2.setJarByClass(WordCount.class);
    job2.setMapperClass(TokenizerMapper2.class);
    job2.setCombinerClass(IntSumReducer2.class);
    job2.setReducerClass(IntSumReducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
        
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]+"/part*"));  // pass the output of the first job for the second job
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
    ControlledJob cJob1 = new ControlledJob(conf);
    cJob1.setJob(job);    
    ControlledJob cJob2 = new ControlledJob(conf);
    cJob2.setJob(job2);

    JobControl jobctrl = new JobControl("jobctrl");
    jobctrl.addJob(cJob1);
    jobctrl.addJob(cJob2);
    cJob2.addDependingJob(cJob1);

    Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
    jobRunnerThread.start();

    while (!jobctrl.allFinished()) {
      System.out.println("Still running...");
      Thread.sleep(5000);
    }
    System.out.println("done");
    jobctrl.stop();
  }
}
  

/**
 * Helping class for running chaining jobs
 * 
 * @author wrpalomino
 * @since 2016-06-29
 */
class JobRunner implements Runnable {
  private JobControl control;

  public JobRunner(JobControl _control) {
    this.control = _control;
  }

  public void run() {
    this.control.run();
  }
}
