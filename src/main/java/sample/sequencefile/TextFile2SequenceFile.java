package sample.sequencefile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class TextFile2SequenceFile {
	
	private static final BytesWritable EMPTY_KEY = new BytesWritable();

	public static void main(String[] args) throws IOException {
		
		String textfile = args[0];

		String uri = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		Text value = new Text();
		SequenceFile.Writer writer = null;
		BufferedReader br = null;
		try {
			GzipCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);
			writer = SequenceFile.createWriter(fs, conf, path, EMPTY_KEY.getClass(),
					value.getClass(), CompressionType.BLOCK, codec);
			br = new BufferedReader(new FileReader(textfile));
			String line = null;
			while( (line=br.readLine()) != null) {
				value.set(line);
				writer.append(EMPTY_KEY, value);
			}
		} finally {
			IOUtils.closeStream(writer);
			IOUtils.closeStream(br);
		}
	}
}

