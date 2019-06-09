package com.ten.aditum.logger.analyze;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Record日志分析
 */
public class MapReduceRecordAnalyzer extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MapReduceRecordAnalyzer(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        String jobName = "aditum_record_logger";
        String inputPath = args[0];
        String outputPath = args[1];
        Path path = new Path(outputPath);
        path.getFileSystem(getConf()).delete(path, true);
        Job job = Job.getInstance(getConf(), jobName);
        job.setJarByClass(MapReduceRecordAnalyzer.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(RecordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(WLA_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 2019-06-09 10:04:40.754 [http-nio-9006-exec-4] INFO  com.ten.aditum.back.controller.RecordController - Record [POST] SUCCESS : Record(id=null, imei=e227b07ebf8f45d58461c5ee3c29ac63, personnelId=99b5f062959641a49cdc2411bdd877a1, visiteTime=2019-06-09 10:04:40.707, visiteStatus=1, isDeleted=0)
     */
    public static class RecordMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String log = value.toString();
            String str = "com.ten.aditum.back.controller.RecordController";
            String baseUrl = "http://www.baidu.cn/";
            int len = str.length();
            int urlLen = baseUrl.length();
            if (log.indexOf(str) != -1) {
                String[] log1 = log.split(str);
                // http://www.baidu.cn/course/jobOffline]
                String[] split2 = log1[1].split("\t");
                String ip = split2[1];// 获取ip
                String url = split2[3];// 获取网址
                String subUrl = "http://www.baidu.cn";
                if (url.length() - 1 > urlLen) {
                    subUrl = url.substring(urlLen, url.length() - 1);
                }
                String result = visitTime + "," + subUrl;
                context.write(new Text(ip), new Text(result));
            }
        }

    }

    /**
     * WLA_Reducer用于处理分组后的数据
     */
    public static class WLA_Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long firstTime = Long.MAX_VALUE;// 首次访问时间
            String startTime = null;
            String endTime = null;
            long lastTime = Long.MIN_VALUE;
            String firstPage = null;// 首次访问页面
            String lastPage = null;
            int count = 0;// 访问页面次数

            for (Text value : values) {
                count++;
                String[] split = value.toString().split(",");

                if (TimeUtil.transDate(split[0]) < firstTime) {
                    firstTime = TimeUtil.transDate(split[0]);// yyyy-MM-dd
                    // HH:mm:ss
                    startTime = split[0].substring(11, 19);
                    firstPage = split[1];
                }

                if (TimeUtil.transDate(split[0]) > lastTime) {
                    lastTime = TimeUtil.transDate(split[0]);
                    endTime = split[0].substring(11, 19);
                    lastPage = split[1];
                }

            } // end for

            long time = 0;
            if ((lastTime - firstTime) % (1000 * 60) > 0) {
                time = (lastTime - firstTime) / (1000 * 60) + 1;
            } else {
                time = (lastTime - firstTime) / (1000 * 60);
            }
            String result = startTime + "\t" + firstPage + "\t" + endTime + "\t" + lastPage + "\t" + count + "\t" + time
                    + "分钟";
            context.write(key, new Text(result));

        }// end reduce

    }// end class

}
