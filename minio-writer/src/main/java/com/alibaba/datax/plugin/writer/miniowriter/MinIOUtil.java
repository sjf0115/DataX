package com.alibaba.datax.plugin.writer.miniowriter;

import com.alibaba.datax.common.util.Configuration;
import io.minio.MinioClient;

public class MinIOUtil {
    // 获取 MinIO 客户端
    public static MinioClient initOssClient(Configuration conf) {
        String endpoint = conf.getString(MinIOConstant.ENDPOINT);
        String accessId = conf.getString(MinIOConstant.ACCESS_ID);
        String accessKey = conf.getString(MinIOConstant.ACCESS_KEY);
        // 客户端
        MinioClient client = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessId, accessKey)
                .build();
        return client;
    }
}
