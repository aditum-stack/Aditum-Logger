package com.ten.aditum.logger.analyze;

import com.ten.aditum.logger.constants.ScheduleConstants;
import com.ten.aditum.logger.constants.SqoopConstants;
import com.ten.aditum.logger.entity.ErrorLog;
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

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * 日志分析
 */
@Slf4j
@Component
@EnableScheduling
@EnableAutoConfiguration
public class MapReduceErrorAnalyzer extends Configured implements Tool {

    @Autowired
    private ErrorLogService errorLogService;

    /**
     * 每天2点分析ERROR日志
     */
    @Scheduled(cron = ScheduleConstants.TIME)
    public void analyze() throws Exception {
        for (int i = 0; i < SqoopConstants.FLUME_LOG_PATHS2.length; i++) {
            String[] args = new String[]{
                    SqoopConstants.FLUME_LOG_PATHS2[i],
                    "output/"
            };
            ToolRunner.run(new MapReduceErrorAnalyzer(), args);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String jobName = "aditum_error_logger";

        String inputPath = args[0];
        String outputPath = args[1];
        Path path = new Path(outputPath);
        path.getFileSystem(getConf()).delete(path, true);
        Job job = Job.getInstance(getConf(), jobName);
        job.setJarByClass(MapReduceErrorAnalyzer.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(ErrorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ErrorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Component
    public static class ErrorMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Autowired
        private ErrorLogService errorLogService;
        private static ErrorLogService errorLogServices;

        @PostConstruct
        public void init() {
            errorLogServices = errorLogService;
        }

        /**
         * 2019-06-08 18:29:25.017 [Druid-ConnectionPool-Create-27818833] ERROR com.alibaba.druid.pool.DruidDataSource - create connection SQLException, url: jdbc:mysql://47.100.236.64:3306/aditum, errorCode 0, state 08S01
         * com.mysql.jdbc.exceptions.jdbc4.CommunicationsException: Communications link failure
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String log = value.toString();
            String logType = "3";
            String str = "] ERROR ";

            if (log.contains(str)) {
                String[] log1 = log.split(str);
                // 分析第一段 2019-06-08 09:57:48
                // 获取访问时间
                String visitTime = log1[0].substring(0, 20);
                // 分析第二段
                String[] split2 = log1[1].split(" ");
                String errorMsg = split2[2];
                if (errorMsg.length() > 500) {
                    errorMsg = errorMsg.substring(0, 500);
                }
                ErrorLog errorLog = new ErrorLog()
                        .setLogTag("Error")
                        .setLogMsg(errorMsg)
                        .setLogStatus(3)
                        .setLogTime(visitTime)
                        .setIsDeleted(0);
                errorLogServices.insert(errorLog);
                context.write(new Text(logType), new Text(errorMsg));
            }
        }
    }

    public static class ErrorReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            log.info("Reducer receive");
        }
    }

}
