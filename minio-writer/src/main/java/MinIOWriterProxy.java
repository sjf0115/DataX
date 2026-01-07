import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.miniowriter.MinIOConstant;
import com.alibaba.datax.plugin.writer.miniowriter.MinioAsyncClient;
import io.minio.CreateMultipartUploadResponse;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.messages.InitiateMultipartUploadResult;
import io.minio.messages.ObjectMetadata;
import io.minio.messages.Part;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/1/3 18:48
 */
public class MinIOWriterProxy {
    private static Logger logger = LoggerFactory.getLogger(MinIOWriterProxy.class);
    private MinioAsyncClient minioClient;
    private Configuration configuration;
    // 是否在服务器端进行加密存储
    private Boolean encrypt;
    private String bucket;

    public MinIOWriterProxy (Configuration configuration, MinioAsyncClient minioClient) {
        this.configuration = configuration;
        this.minioClient = minioClient;
        this.encrypt = configuration.getBool(MinIOConstant.ENCRYPT, false);
        this.bucket = configuration.getString(MinIOConstant.BUCKET);
    }

    public InitiateMultipartUploadResult initiateMultipartUpload(String bucketName, String objectName) throws Exception {
        CompletableFuture<CreateMultipartUploadResponse> multipartUploadAsync = minioClient.createMultipartUploadAsync(
                bucketName,
                null,
                objectName,
                null,
                null
        );
        CreateMultipartUploadResponse multipartUploadResponse = multipartUploadAsync.get();
        InitiateMultipartUploadResult result = multipartUploadResponse.result();
        return result;
    }

    public ObjectWriteResponse completeMultipartUpload(String bucketName, String objectName, String uploadId, Part[] parts) throws Exception {
        CompletableFuture<ObjectWriteResponse> completableFuture = minioClient.completeMultipartUploadAsync(
                bucketName,
                null,
                objectName,
                uploadId,
                parts,
                null,
                null
        );
        ObjectWriteResponse response = completableFuture.get();
        return response;
    }
}
