package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;

public class Main {

    public static void main(String[] args) {
        try {
            Configuration conf0 = new Configuration();
            conf0.set("es.nodes", "192.168.169.110:9200");
            conf0.set("es.resource", "customer/external");
            conf0.setBoolean("mapred.map.tasks.speculative.execution", false);
            conf0.setBoolean("mapred.reduce.tasks.speculative.execution", false);

            if(args[0].equals("import_to_es"))
            {
                Job job0 = new Job(conf0, "es");
                job0.setJarByClass(Main.class);
                job0.setMapOutputKeyClass(Text.class);
                job0.setMapOutputValueClass(Text.class);
                job0.setOutputKeyClass(NullWritable.class);
                job0.setOutputValueClass(Text.class);
                job0.setNumReduceTasks(0);
                job0.setMapperClass(esMapImport.class);
                job0.setOutputFormatClass(EsOutputFormat.class);
                FileInputFormat.addInputPath(job0, new Path(args[1]));
                job0.setSpeculativeExecution(false);
                job0.waitForCompletion(true);
            }

            if(args[0].equals("export_to_hdfs"))
            {
                Job job0 = new Job(conf0, "es");
                job0.setJarByClass(Main.class);
                job0.setMapOutputKeyClass(Text.class);
                job0.setMapOutputValueClass(LongWritable.class);
                job0.setMapperClass(esMapExport.class);
                job0.setReducerClass(esReducerExport.class);
                job0.setInputFormatClass(EsInputFormat.class);
                job0.setOutputKeyClass(Text.class);
                job0.setOutputValueClass(LongWritable.class);

                FileOutputFormat.setOutputPath(job0, new Path(args[1]));

                job0.setSpeculativeExecution(false);
                job0.waitForCompletion(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
