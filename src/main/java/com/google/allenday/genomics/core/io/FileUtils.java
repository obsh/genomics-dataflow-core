package com.google.allenday.genomics.core.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {

    private static Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public static void saveDataToFile(byte[] data, String filename) throws IOException {
        File destFile = new File(filename);
        destFile.createNewFile();
        OutputStream os = new FileOutputStream(new File(filename));
        os.write(data);
        os.close();
    }

    public static boolean mkdir(String path) {
        Path dir;
        if (path.charAt(path.length() - 1) != '/') {
            dir = Paths.get(path).getParent();
        } else {
            dir = Paths.get(path);
        }
        try {
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
                LOG.info(String.format("Dir %s created", dir.toString()));
            }
            return true;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            return false;
        }
    }

    public static byte[] readFileToByteArray(String filePath) throws IOException {
        return Files.readAllBytes(new File(filePath).toPath());
    }

    public static long getFileSizeMegaBytes(String filePath) {
        return new File(filePath).length() / (1024 * 1024);
    }

    public static String getFilenameFromPath(String filePath) {
        if (filePath.contains("/")) {
            return filePath.split("/")[filePath.split("/").length - 1];
        } else {
            return filePath;
        }
    }

    public static String changeFileExtension(String fileName, String newFileExtension) {
        if (fileName.contains(".")) {
            return fileName.split("\\.")[0] + newFileExtension;
        } else {
            return fileName + newFileExtension;
        }
    }


    public static void deleteFile(String filePath){
        File fileToDelete = new File(filePath);
        if (fileToDelete.exists()){
            boolean delete = fileToDelete.delete();
            if (delete) {
                LOG.info(String.format("File %s deleted", filePath));
            }
        }
    }

    public static void deleteDir(String dirPath){
        try {
            org.apache.commons.io.FileUtils.deleteDirectory(new File(dirPath));
            LOG.info(String.format("Dir %s deleted", dirPath));
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    public static long getFreeDiskSpace(){
        return new File("/").getFreeSpace();
    }
}
