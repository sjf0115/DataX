package com.alibaba.datax.plugin.writer.miniowriter;

import com.google.common.collect.Multimap;
import io.minio.*;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Part;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;

/**
 * 功能：自定义扩展 Minio Client
 *      扩展 MinioAsyncClient 主要为了解决部分分片上传方法为 protected 无法调用的问题。
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/1/3 17:42
 */
public class MinioAsyncClient extends io.minio.MinioAsyncClient {
    protected MinioAsyncClient(io.minio.MinioAsyncClient client) {
        super(client);
    }

    /**
     * 创建分片上传请求
     * @param bucketName       存储桶
     * @param region           区域
     * @param objectName       对象名
     * @param headers          消息头
     * @param extraQueryParams 额外查询参数
     */
    @Override
    public CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadAsync(String bucketName, String region, String objectName, Multimap<String, String> headers, Multimap<String, String> extraQueryParams) throws InsufficientDataException, InternalException, InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
        return super.createMultipartUploadAsync(bucketName, region, objectName, headers, extraQueryParams);
    }

    /**
     * 完成分片上传，执行合并文件
     * @param bucketName       存储桶
     * @param region           区域
     * @param objectName       对象名
     * @param uploadId         上传ID
     * @param parts            分片
     * @param extraHeaders     额外消息头
     * @param extraQueryParams 额外查询参数
     */
    @Override
    public CompletableFuture<ObjectWriteResponse> completeMultipartUploadAsync(String bucketName, String region, String objectName, String uploadId, Part[] parts, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams) throws InsufficientDataException, InternalException, InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
        return super.completeMultipartUploadAsync(bucketName, region, objectName, uploadId, parts, extraHeaders, extraQueryParams);
    }

    /**
     * 终止分片上传
     * @param bucketName       存储桶
     * @param region           区域
     * @param objectName       对象名
     * @param uploadId         上传ID
     * @param extraHeaders     额外消息头(可选)
     * @param extraQueryParams 额外查询参数(可选)
     */
    @Override
    protected CompletableFuture<AbortMultipartUploadResponse> abortMultipartUploadAsync(String bucketName, String region, String objectName, String uploadId, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams) throws InsufficientDataException, InternalException, InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
        return super.abortMultipartUploadAsync(bucketName, region, objectName, uploadId, extraHeaders, extraQueryParams);
    }

    /**
     * 查询分片数据
     * @param bucketName       存储桶
     * @param region           区域
     * @param objectName       对象名
     * @param maxParts         抓取的最大分片数量.
     * @param partNumberMarker 分片数量创建器.
     * @param uploadId         上传ID
     * @param extraHeaders     额外消息头
     * @param extraQueryParams 额外查询参数
     */
    @Override
    public CompletableFuture<ListPartsResponse> listPartsAsync(String bucketName, String region, String objectName, Integer maxParts, Integer partNumberMarker, String uploadId, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams) throws InsufficientDataException, InternalException, InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
        return super.listPartsAsync(bucketName, region, objectName, maxParts, partNumberMarker, uploadId, extraHeaders, extraQueryParams);
    }

    /**
     * 查询分片数据
     * @param bucketName Name of the bucket.
     * @param region Region of the bucket (Optional).
     * @param delimiter Delimiter (Optional).
     * @param encodingType Encoding type (Optional).
     * @param keyMarker Key marker (Optional).
     * @param maxUploads Maximum upload information to fetch (Optional).
     * @param prefix Prefix (Optional).
     * @param uploadIdMarker Upload ID marker (Optional).
     * @param extraHeaders Extra headers for request (Optional).
     * @param extraQueryParams Extra query parameters for request (Optional).
     */
    @Override
    protected CompletableFuture<ListMultipartUploadsResponse> listMultipartUploadsAsync(String bucketName, String region, String delimiter, String encodingType, String keyMarker, Integer maxUploads, String prefix, String uploadIdMarker, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams) throws InsufficientDataException, InternalException, InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
        return super.listMultipartUploadsAsync(bucketName, region, delimiter, encodingType, keyMarker, maxUploads, prefix, uploadIdMarker, extraHeaders, extraQueryParams);
    }
}
