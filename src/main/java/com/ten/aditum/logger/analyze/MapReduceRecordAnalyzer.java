package com.ten.aditum.logger.analyze;

import com.ten.aditum.logger.constants.ScheduleConstants;
import com.ten.aditum.logger.constants.SqoopConstants;
import com.ten.aditum.logger.service.ErrorLogService;
import lombok.extern.slf4j.Slf4j;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Record日志分析
 */
@Slf4j
@Component
@EnableScheduling
@EnableAutoConfiguration
public class MapReduceRecordAnalyzer extends Configured implements Tool {

    @Autowired
    private ErrorLogService errorLogService;

    /**
     * 每天2点分析ERROR日志
     */
    @Scheduled(cron = ScheduleConstants.TIME)
    public void analyze() throws Exception {
        for (int i = 0; i < SqoopConstants.FLUME_LOG_PATHS.length; i++) {
            String[] args = new String[]{
                    SqoopConstants.FLUME_LOG_PATHS[i],
                    "output/"
            };
            ToolRunner.run(new MapReduceRecordAnalyzer(), args);
        }
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
        job.setReducerClass(RecordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 2019-06-09 10:04:40.754 [http-nio-9006-exec-4] INFO
     * com.ten.aditum.back.controller.RecordController - Record [POST] SUCCESS : Record(id=null, imei=e227b07ebf8f45d58461c5ee3c29ac63, personnelId=99b5f062959641a49cdc2411bdd877a1, visiteTime=2019-06-09 10:04:40.707, visiteStatus=1, isDeleted=0)
     */
    public class RecordMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String log = value.toString();
            String str = "com.ten.aditum.back.controller.RecordController";
            String baseUrl = "Record";
            int len = str.length();
            int urlLen = baseUrl.length();
            if (log.contains(str)) {
                String[] log1 = log.split(str);
                String[] split2 = log1[1].split("\t");
                String ip = split2[1];
                String url = split2[3];
                String subUrl = "Record";
                if (url.length() - 1 > urlLen) {
                    subUrl = url.substring(urlLen, url.length() - 1);
                }
                String result = subUrl;
                context.write(new Text(ip), new Text(result));
            }
        }

    }

    public class RecordReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long firstTime = Long.MAX_VALUE;
            String startTime = null;
            String endTime = null;
            long lastTime = Long.MIN_VALUE;
            String firstPage = null;
            String lastPage = null;
            int count = 0;
            for (Text value : values) {
                count++;
                String[] split = value.toString().split(",");
                if (TimeUtil.transDate(split[0]) < firstTime) {
                    // yyyy-MM-dd
                    firstTime = TimeUtil.transDate(split[0]);
                    // HH:mm:ss
                    startTime = split[0].substring(11, 19);
                    firstPage = split[1];
                }

                if (TimeUtil.transDate(split[0]) > lastTime) {
                    lastTime = TimeUtil.transDate(split[0]);
                    endTime = split[0].substring(11, 19);
                    lastPage = split[1];
                }
            }

            long time = 0;
            if ((lastTime - firstTime) % (1000 * 60) > 0) {
                time = (lastTime - firstTime) / (1000 * 60) + 1;
            } else {
                time = (lastTime - firstTime) / (1000 * 60);
            }
            String result = startTime + "\t" + firstPage + "\t" + endTime + "\t" + lastPage + "\t" + count + "\t" + time
                    + "分钟";
            context.write(key, new Text(result));
        }
    }

}
