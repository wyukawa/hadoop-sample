package sample.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCountDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("word count");
		conf.setMapperClass(WordCountMapper.class);
		conf.setReducerClass(WordCountReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
	}

}
