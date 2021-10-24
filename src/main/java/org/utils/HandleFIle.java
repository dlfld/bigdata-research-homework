package org.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author dailinfeng
 * @Description TODO hdfs文件处理
 * @Date 2021/10/17 1:53 下午
 * @Version 1.0
 */
public class HandleFIle {
    /**
     * 删除文件夹
     *
     * @param path
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    public void del(String path, String url) throws IOException, InterruptedException, URISyntaxException {
        //删除文件（夹）
        Configuration conf = new Configuration();//加载配置
        FileSystem fs = FileSystem.get(new URI(url), conf, "root");//加载文件系统实例
        fs.delete(new Path(path), true);
    }

    /**
     * 上传文件
     *
     * @throws Exception
     */
    public void upload(String url, String filePath, String file) throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        URI uri = new URI(url);
        FileSystem fs = FileSystem.get(uri, conf, "root");
        Path resP = new Path(filePath);
        Path destP = new Path(file);
        if (!fs.exists(destP)) {
            fs.mkdirs(destP);
        }
        fs.copyFromLocalFile(resP, destP);
        fs.close();
    }


    public static void main(String[] args) throws Exception {
        new HandleFIle().del("/exp2_out", "hdfs://10.23.71.70:9000");
        new HandleFIle().del("/exp2_out1", "hdfs://10.23.71.70:9000");
        new HandleFIle().del("/exp2_out2", "hdfs://10.23.71.70:9000");
        new HandleFIle().del("/exp2_out3", "hdfs://10.23.71.70:9000");
//        new HandleFIle().upload("hdfs://10.23.71.70:9000", "/Users/dailinfeng/Documents/学习/研一上/大数据技术与应用/HDFS/src/main/java/org/example/bbb.txt", "/ex2");
    }
}
