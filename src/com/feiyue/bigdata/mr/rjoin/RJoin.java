package com.feiyue.bigdata.mr.rjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class RJoin {

    static class RJoinMapper extends Mapper<LongWritable,Text,Text,InfoBean>{

        InfoBean infoBean = new InfoBean();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String name = inputSplit.getPath().getName();

            //prodoctId
            String pid = "";

            //订单信息 order.txt
            if(name.startsWith("order")){
                String[] fields = line.split(",");
                pid = fields[2];
                // id date pid amount
                infoBean.set(Integer.parseInt(fields[0]),fields[1],pid,Integer.parseInt(fields[3]),"",0,0,"0");
            }else {
                //商品信息 product.txt
                String[] fields = line.split(",");
                pid = fields[0];

                // pid pname category_id price
                infoBean.set(0,"",pid,0,fields[1],Integer.parseInt(fields[2]),Float.parseFloat(fields[3]),"1");
            }

            k.set(pid);

            context.write(k,infoBean);

        }
    }

    static class RJoinReducer extends Reducer<Text,InfoBean,InfoBean,NullWritable>{


        @Override
        protected void reduce(Text key, Iterable<InfoBean> beans, Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean();
            ArrayList<InfoBean> orderBeans = new ArrayList<>();

            for (InfoBean infoBean : beans){
                if ("1".equals(infoBean.getFlag())){
                    try {
                        BeanUtils.copyProperties(pdBean,infoBean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }

                }else{
                    InfoBean odBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(odBean,infoBean);
                        orderBeans.add(odBean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }

                }

            }

            for (InfoBean bean : orderBeans){
                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());

                context.write(bean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(RJoin.class);

        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 判断output文件夹是否存在，如果存在则删除
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
