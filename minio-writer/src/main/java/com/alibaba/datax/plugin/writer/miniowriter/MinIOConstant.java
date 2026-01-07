package com.alibaba.datax.plugin.writer.miniowriter;

/**
 * 功能：Constant
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/1/2 10:35
 */
public class MinIOConstant {
    // 必填基础参数
    public static final String ENDPOINT = "endpoint";
    public static final String ACCESS_ID = "accessId";
    public static final String ACCESS_KEY = "accessKey";
    public static final String BUCKET = "bucket";

    public static final String OBJECT = "object";
    public static final String CNAME = "cname";
    public static final String PARTITION = "partition";


    public static final String MINIO_CONFIG = "minio";
    public static final String POSTGRESQL_CONFIG = "postgresql";

    /**
     * 多个task是否写单个object文件：
     * false 多个task写多个object（默认是false, 保持向前兼容）
     * true 多个task写单个object
     */
    public static final String WRITE_SINGLE_OBJECT = "writeSingleObject";

    /**
     * encrypt: 是否需要将数据在oss上加密存储
     */
    public static final String ENCRYPT = "encrypt";
}
