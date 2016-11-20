package hdfs;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;

import java.io.IOException;

public class esMapExport extends Mapper<Object, Object, Text, LongWritable> {
    @Override
    protected void map(Object key, Object value, Context context)
            throws IOException, InterruptedException {

        MapWritable document = (MapWritable) value;
        JSONObject obj = es2Json(document);
        context.write(new Text(obj.get("word").toString()), new LongWritable(1));
    }

    public JSONObject es2Json(MapWritable valueWritable) throws IOException, InterruptedException {
        JSONObject obj = new JSONObject();
        for (Writable keyWritable : valueWritable.keySet()) {
            String key = ((Text) keyWritable).toString();
            Writable valWritable = valueWritable.get(keyWritable);
            obj.put(key, valWritable.toString());
        }
        return obj;
    }
}
