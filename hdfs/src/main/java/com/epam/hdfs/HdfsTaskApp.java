package com.epam.hdfs;

import com.epam.kerberos.HadoopKerberosUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HdfsTaskApp {
    private static final String HDFS_URL = "hdfs://svqxbdcn6hdp25secn1.pentahoqa.com:8020";
    private static final String WEB_HDFS_URL = "webhdfs://svqxbdcn6hdp25n1.pentahoqa.com:8020";

    public static void main(String[] args) {
        Configuration configuration = createConfig();
        try {
            HadoopKerberosUtil.doLogin("devuser@PENTAHOQA.COM", "password");
        } catch (LoginException | IOException e) {
            e.printStackTrace();
        }

        try(FileSystem fileSystem = FileSystem.get(configuration)){
            //copyToLocalFile("/wordcount/input/pg20417.txt", "D:/test_data/input.txt", fileSystem);
            //copyFromLocal("D:/test_data/input.txt", "/user/Stanislau_Hramatskiu/task/test.txt", fileSystem);
            //deleteFile("/user/Stanislau_Hramatskiu/task/test.txt", fileSystem);
            fileSystem.mkdirs(new Path("/wordcount/test"));
            copyFromLocal("D:/test_data/input.txt", "/wordcount/test/test.txt", fileSystem);
            //FileStatus fileStatus = getFileStatus("/user/Stanislau_Hramatskiu/task/test.txt", fileSystem);
            List<String> hostNames = getDataNodesHostNames(fileSystem);
            hostNames.forEach(System.out::println);
            //FSDataInputStream inputStream = readFile("/user/Stanislau_Hramatskiu/task/test.txt", fileSystem);

            //copyToLocalFile(args[0], args[1], fileSystem);
        }
        catch (IOException | ArrayIndexOutOfBoundsException ex){
            System.out.println(ex.getMessage());
        }

        //configuration.set("fs.default.name", WEB_HDFS_URL);
        /*configuration.addResource("hdfs-site.xml");
        configuration.set("fs.defaultFS", WEB_HDFS_URL);
        try(WebHdfsFileSystem fileSystem = (WebHdfsFileSystem) FileSystem.get(configuration)) {
            //FileStatus fileStatus = getWithRestApi("/wordcount/task/input.txt", fileSystem);
            //FsStatus status = fileSystem.getStatus();
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
            //System.out.println(status);
        }
        catch (IOException ex){
            System.out.println(ex.getMessage());
            //logging
        }*/
    }

    private static Configuration createConfig(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", HDFS_URL);
        //configuration.set("hadoop.job.ugi", "cloudera");
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.addResource("hdfs-site.xml");
        configuration.addResource("core-site.xml");

        return configuration;
    }

    private static void copyToLocalFile(String source, String dest, FileSystem fileSystem) throws IOException{
        if (fileSystem.exists(new Path(source))){
            fileSystem.copyToLocalFile(new Path(source), new Path(dest));
        }
    }

    private static void copyFromLocal(String source, String dest, FileSystem fileSystem) throws IOException{
        if (!fileSystem.exists(new Path(dest))){
            fileSystem.copyFromLocalFile(new Path(source), new Path(dest));
        }
    }

    private static void moveFile(String oldFilePath, String newFilePath, FileSystem fileSystem) throws IOException{
        if (fileSystem.exists(new Path(oldFilePath)) || fileSystem.exists(new Path(newFilePath))){
            fileSystem.rename(new Path(oldFilePath), new Path(newFilePath));
        }
    }

    private static FSDataOutputStream createFile(String filePath, boolean overwrite, FileSystem fileSystem) throws IOException{
        return fileSystem.create(new Path(filePath), overwrite);
    }

    private static void deleteFile(String filePath, FileSystem fileSystem) throws IOException{
        fileSystem.deleteOnExit(new Path(filePath));
    }

    private static FileStatus getFileStatus(String filePath, FileSystem fileSystem) throws IOException{
        return fileSystem.exists(new Path(filePath)) ? fileSystem.getFileStatus(new Path(filePath)) : null;
    }

    private static List<String> getDataNodesHostNames(FileSystem fileSystem) throws IOException{
        return Arrays.stream(((DistributedFileSystem) fileSystem).getDataNodeStats()).map(DatanodeInfo::getHostName).collect(Collectors.toList());
    }

    private static RemoteIterator<CachePoolEntry> getDifferentDataInf(FileSystem fileSystem) throws IOException{
        //return ((DistributedFileSystem) fileSystem).getStatus();
        return  ((DistributedFileSystem) fileSystem).listCachePools();
    }

    private static FSDataInputStream readFile(String filePath, FileSystem fileSystem) throws IOException{
        return fileSystem.exists(new Path(filePath)) ? fileSystem.open(new Path(filePath)) : null;
    }

    private static FileStatus getWithRestApi(String filePath, WebHdfsFileSystem fileSystem) throws IOException{
        return fileSystem.getFileStatus(new Path(filePath));
    }

    private static void readFromPosWithRestApi(String filePath, WebHdfsFileSystem fileSystem) throws IOException{
        fileSystem.createNewFile(new Path(filePath));
    }
}
