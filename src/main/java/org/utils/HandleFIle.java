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
    public void del(String path) throws IOException, InterruptedException, URISyntaxException {
        //删除文件（夹）
        Configuration conf = new Configuration();//加载配置
        FileSystem fs = FileSystem.get(new URI("hdfs://10.23.71.70:9000/"), conf, "root");//加载文件系统实例
        fs.delete(new Path(path), true);
    }

    /**
     * 上传文件
     *
     * @throws Exception
     */
    public void upload() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        URI uri = new URI("hdfs://10.23.71.70:9000");
        FileSystem fs = FileSystem.get(uri, conf, "root");
        Path resP = new Path("/Users/dailinfeng/Desktop/小项目/HDFS/src/main/java/org/example/aaa.txt");
        Path destP = new Path("/ex1");
        if (!fs.exists(destP)) {
            fs.mkdirs(destP);
        }
        fs.copyFromLocalFile(resP, destP);
        fs.close();
    }


    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        new HandleFIle().del("/ex1");
    }
}
