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
 * 日志分析
 */
public class MapReduceLogAnalyzer2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MapReduceLogAnalyzer2(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        String jobName = "aditum_logger";

        String inputPath = args[0];
        String outputPath = args[1];
        Path path = new Path(outputPath);
        // 删除输出目录
        path.getFileSystem(getConf()).delete(path, true);

        // 1、把所有代码组织到类似于Topology的类中
        Job job = Job.getInstance(getConf(), jobName);

        // 2、一定要打包运行，必须写下面一行代码
        job.setJarByClass(MapReduceLogAnalyzer2.class);

        // 3、指定输入的hdfs
        FileInputFormat.setInputPaths(job, inputPath);

        // 4、指定map类
        job.setMapperClass(ErrorLogMapper.class);

        // 5、指定map输出的<key,value>的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 6、指定reduce类
        job.setReducerClass(ErrorLogReducer.class);

        // 7、指定reduce输出的<key,value>的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 8、指定输出的hdfs
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 分析ERROR日志
     */
    public static class ErrorLogMapper extends Mapper<LongWritable, Text, Text, Text> {

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
            if (log.indexOf(str) != -1) {
                String[] log1 = log.split(str);
                // 分析第一段 2019-06-08 09:57:48
                // 获取访问时间
                String visitTime = log1[0].substring(0, 20);
                // 分析第二段
                String[] split2 = log1[1].substring("\t");
                String errorMsg = split2[1];
                context.write(new Text(logType), new Text(errorMsg));
            }
        }

    }

    /**
     * WLA_Reducer用于处理分组后的数据
     */
    public static class ErrorLogReducer extends Reducer<Text, Text, Text, Text> {

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
