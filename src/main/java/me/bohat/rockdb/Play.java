package me.bohat.rockdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class Play {
    static Logger logger = LoggerFactory.getLogger(Play.class);
    static String dir = "C:\\Users\\phone\\IdeaProjects\\rockdb\\src\\main\\resources\\rockdb";
    public static void main(String[] args) {
        System.out.println("Start engine");
        RocksDB db = initialize(dir,"MARK");
        put(db,"001","I feeling bad today");
        System.out.println(get(db,"001"));
        System.out.println(estimateNumberOfKey(db));

    }
    public static RocksDB initialize(String path,String name){
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
        File dbDir = new File(path,name);
        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
            return RocksDB.open(options,dbDir.getAbsolutePath());
        }catch (IOException | RocksDBException e){
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
    public static synchronized void put(RocksDB db,String key,String value){
        logger.debug("Trying to save data into database [key={}]",key);
        try {
            db.put(key.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));
        }catch (RocksDBException e){
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
        logger.debug("Save data into database successfully [key={}]",key);

    }
    public static String get(RocksDB db,String key){
        try {
            byte[] bytes = db.get(key.getBytes(StandardCharsets.UTF_8));
            if (bytes == null) return null;
            return new String(bytes,StandardCharsets.UTF_8);
        }catch (RocksDBException e){
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
    public static String estimateNumberOfKey(RocksDB db){
        try {
            return db.getProperty("rocksdb.estimate-num-keys");
        }catch (RocksDBException e){
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
