package com.feiyue.bigdata.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;

public class FlowCount {

    static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean>{

        FlowBean flowBean = new FlowBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            String phoneNO = fields[1];
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length -2]);

            flowBean.setUpFlow(upFlow);
            flowBean.setDownFlow(downFlow);

            context.write(new Text(phoneNO),flowBean);

        }

    }

    static  class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        FlowBean resultBean = null;
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upflow = 0;
            long sum_downflow = 0;

            for (FlowBean flowBean : values){
                sum_upflow += flowBean.getUpFlow();
                sum_downflow += flowBean.getDownFlow();
            }

//            resultBean.setUpFlow(sum_upflow);
//            resultBean.setDownFlow(sum_downflow);
            resultBean = new FlowBean(sum_upflow,sum_downflow);


            context.write(key,resultBean);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowCount.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 判断output文件夹是否存在，如果存在则删除
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }

        boolean res = job.waitForCompletion(true);
        System.exit(res?1:0);
    }
}
