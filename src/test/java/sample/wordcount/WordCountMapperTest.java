package sample.wordcount;

import static org.mockito.Mockito.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import sample.wordcount.WordCountMapper;

public class WordCountMapperTest {

	@Test
	public void testMapObjectTextContext() throws Exception {
		
		WordCountMapper wordCountMapper = new WordCountMapper();
		
		Text value = new Text("aaa");
		
		OutputCollector output = mock(OutputCollector.class);
		
		wordCountMapper.map(null, value, output, null);
		
		verify(output).collect(value, new IntWritable(1));
		
	}

}
