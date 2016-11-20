package hdfs;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;

public class esMapImport extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String text = value.toString().replaceAll("(\\r|\\n|\\r\\n)+", "\\s");
        String[] values = text.split("\\s");
        for (String v : values) {
            MapWritable doc = new MapWritable();
            doc.put(new Text("word"), new Text(v));
            context.write(NullWritable.get(), doc);
        }
    }
}
