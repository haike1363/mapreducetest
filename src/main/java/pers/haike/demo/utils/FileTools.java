package pers.haike.demo.utils;

import java.io.File;

public class FileTools {
    public static void delFile(File file) {
        if (!file.exists()) {
            return;
        }

        if (file.isFile()) {
            // 文件: 直接删除
            file.delete();
        } else if (file.isDirectory()) {
            // 文件夹
            // 1. 删除子文件
            for (File f : file.listFiles()) {
                delFile(f);
            }
            // 2. 删除文件夹
            file.delete();
        }
    }
}
