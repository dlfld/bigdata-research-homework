package org.experiment.exp2.Q3;

import org.utils.HandleFIle;

/**
 * @Author dailinfeng
 * @Description TODO
 * @Date 2021/10/24 9:27 上午
 * @Version 1.0
 */
public class FileUpload {
    public static void main(String[] args) throws Exception {
        HandleFIle upload = new HandleFIle();
        upload.upload("hdfs://10.23.71.70:9000","/Users/dailinfeng/Documents/学习/研一上/大数据技术与应用/HDFS/src/main/java/org/experiment/exp2/Q3/A.txt","/exp2_Q3");
        upload.upload("hdfs://10.23.71.70:9000","/Users/dailinfeng/Documents/学习/研一上/大数据技术与应用/HDFS/src/main/java/org/experiment/exp2/Q3/B.txt","/exp2_Q3");
        upload.upload("hdfs://10.23.71.70:9000","/Users/dailinfeng/Documents/学习/研一上/大数据技术与应用/HDFS/src/main/java/org/experiment/exp2/Q3/C.txt","/exp2_Q3");

    }
}
