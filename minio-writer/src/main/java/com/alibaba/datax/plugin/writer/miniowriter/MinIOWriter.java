package com.alibaba.datax.plugin.writer.miniowriter;


import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.plugin.unstructuredstorage.FileFormat;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.binaryFileUtil.BinaryFileWriterUtil;
import com.alibaba.fastjson2.JSON;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.errors.*;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.SOURCE_FILE_NAME;
import static com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.TRUNCATE;

/**
 * 功能：MinIO Writer
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/12/30 23:09
 */
public class MinIOWriter extends Writer {

    /**
     * Job
     */
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private Configuration peerPluginJobConf;
        private MinioClient minioClient;

        private Boolean isBinaryFile;
        private String objectDir;
        private String syncMode;
        private String fileFormat;
        private String encoding;
        private String bucket;
        private String object;
        private boolean writeSingleObject;
        private List<String> header;

        @Override
        public void preHandler(Configuration config) {
            // Writer 参数
            Configuration writerParamsConf = config.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);
            Configuration writerOssPluginConf = writerParamsConf.getConfiguration(MinIOConstant.MINIO_CONFIG);

            Configuration newWriterParamsConf = Configuration.newDefault();
            config.remove(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);

            // 将 postgresqlwriter 的 pg 配置注入到 postgresqlConfig 中, 供后面的postHandler使用 ?
            writerParamsConf.remove(MinIOConstant.MINIO_CONFIG);
            newWriterParamsConf.set(MinIOConstant.POSTGRESQL_CONFIG, writerParamsConf);
            newWriterParamsConf.merge(writerOssPluginConf, true);
            // 设置 Writer 名称
            config.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME, "miniowriter");
            // 重构 Writer 参数
            config.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER, newWriterParamsConf);
            LOG.info("MinIOWriter Phase 1 preHandler end... ");
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.peerPluginJobConf = this.getPeerPluginJobConf();

            // 基础参数检查
            this.basicValidateParameter();
            // 获取文件格式
            this.fileFormat = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT);
            // 获取编码
            this.encoding = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.ENCODING,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_ENCODING);
            // 是否是二进制文件
            this.isBinaryFile = FileFormat.getFileFormatByConfiguration(this.peerPluginJobConf).isBinary();
            // 同步模式
            this.syncMode = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.SYNC_MODE, "");
            // 是否写入单文件
            this.writeSingleObject = this.writerSliceConfig.getBool(
                    MinIOConstant.WRITE_SINGLE_OBJECT, false);
            this.header = this.writerSliceConfig.getList(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.HEADER,
                    null,
                    String.class);
            this.validateParameter();
            this.minioClient = MinIOUtil.initOssClient(this.writerSliceConfig);
            //this.ossWriterProxy = new OssWriterProxy(this.writerSliceConfig, this.ossClient);
        }

        @Override
        public void prepare() {
            LOG.info("begin do prepare...");
            this.bucket = this.writerSliceConfig.getString(MinIOConstant.BUCKET);
            this.object = this.writerSliceConfig.getString(MinIOConstant.OBJECT);
            // 写入模式 truncate、append、nonConflict
            String writeMode = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            List<String> sourceFileName = this.peerPluginJobConf.getList(SOURCE_FILE_NAME, new ArrayList<String>(), String.class);
            this.objectDir = this.getObjectDir(object);

            String fullObjectName = null;
            String truncateMode = this.writerSliceConfig.getString("truncateMode", "objectMatch");
            // 前缀删除模式
            if ("prefix".equalsIgnoreCase(truncateMode)) {
                BinaryFileWriterUtil.checkFileNameIfRepeatedThrowException(sourceFileName);
                if (TRUNCATE.equals(writeMode)) {
                    LOG.info("You have configured [writeMode] [truncate], so the system will start to clear the objects starting with [{}] under [{}]. ", bucket, object);
                    // 第一步：查询所有前缀的Object
                    LOG.info("list objects with listObjects(bucket, object)");
                    ListObjectsArgs objectsArgs = ListObjectsArgs.builder().bucket(bucket).prefix(object).recursive(true).build();
                    Iterable<Result<Item>> results = minioClient.listObjects(objectsArgs);

                    // 第二步：删除Object
                    List<DeleteObject> objects2Delete = new LinkedList<>();
                    for (Result<Item> result : results) {
                        try {
                            Item item = result.get();
                            objects2Delete.add(new DeleteObject(item.objectName()));
                        } catch (Exception e) {
                            LOG.warn("Error in list object: {}", e.getMessage());
                        }
                    }
                    RemoveObjectsArgs deleteObjectsArgs = RemoveObjectsArgs.builder().bucket(bucket).objects(objects2Delete).build();
                    Iterable<Result<DeleteError>> deleteResults = minioClient.removeObjects(deleteObjectsArgs);
                    for (Result<DeleteError> result : deleteResults) {
                        DeleteError error = null;
                        try {
                            error = result.get();
                        } catch (Exception e) {
                            LOG.warn("Error in deleting object {}, {}", error.objectName(), error.message());
                        }
                    }
                } else {
                    throw DataXException.asDataXException(MinIOWriterErrorCode.ILLEGAL_VALUE,
                            "only support truncate writeMode in copy sync mode.");
                }
            } else {
                /*if (TRUNCATE.equals(writeMode)) {
                    sourceFileName = this.peerPluginJobConf.getList(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.SOURCE_FILE, new ArrayList<String>(), String.class);
                    List<String> readerPath =  this.peerPluginJobConf.getList(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.PATH, new ArrayList<String>(), String.class);
                    int parentPathLength = OssWriter.parseParentPathLength(readerPath);
                    this.writerSliceConfig.set("__parentPathLength", parentPathLength);
                    BinaryFileWriterUtil.checkFileNameIfRepeatedThrowException(sourceFileName);

                    // 原样文件名删除模式
                    int splitCount = sourceFileName.size() / 1000 + 1;
                    List<List<String>> splitResult = RangeSplitUtil.doListSplit(sourceFileName, splitCount);
                    for (List<String> eachSlice : splitResult) {
                        assert eachSlice.size() <= 1000;
                        if (eachSlice.isEmpty()) {
                            continue;
                        }
                        List<String> ossObjFullPath = new ArrayList<String>();
                        for (String eachObj : eachSlice) {
                            fullObjectName = String.format("%s%s", objectDir, eachObj.substring(parentPathLength, eachObj.length()));
                            ossObjFullPath.add(fullObjectName);
                        }
                        LOG.info(String.format("[origin object name truncate mode]delete oss object [%s].", JSON.toJSONString(ossObjFullPath)));
                        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
                        deleteRequest.setKeys(ossObjFullPath);
                        deleteRequest.setQuiet(true);// 简单模式
                        DeleteObjectsResult deleteResult = this.ossClient.deleteObjects(deleteRequest);
                        assert deleteResult.getDeletedObjects().isEmpty();
                        LOG.warn("OSS request id:{}, objects delete failed:{}", deleteResult.getRequestId(),
                                JSON.toJSONString(deleteResult.getDeletedObjects()));
                    }
                } else {
                    throw DataXException.asDataXException(MinIOWriterErrorCode.ILLEGAL_VALUE,
                            "only support truncate writeMode in copy sync mode.");
                }*/
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            if(this.isPeer2PeerCopyMode()){
                // Reader 配置
                List<Configuration> readerSplitConfigs = this.getReaderPluginSplitConf();
                for (int i = 0; i < readerSplitConfigs.size(); i++) {
                    Configuration sliceConfig = writerSliceConfig.clone();
                    sliceConfig.set(MinIOConstant.OBJECT, objectDir);
                    sliceConfig.set(com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.BINARY, this.isBinaryFile);
                    writerSplitConfigs.add(sliceConfig);
                }
            } else {
                if (this.writeSingleObject) {
                    writerSplitConfigs = doSplitForWriteSingleObject(mandatoryNumber);
                } else {
                    writerSplitConfigs = doSplitForWriteMultiObject(mandatoryNumber);
                }
            }
            LOG.info("end do split. split size: {}", writerSplitConfigs.size());
            return writerSplitConfigs;
        }

        @Override
        public void post() {
            /*if (this.writeSingleObject) {
                // 写入单个文件
                try {
                    // 第一步：合并上传最后一个block
                    LOG.info("Has upload part size: {}", OssSingleObject.allPartETags.size());
                    if (OssSingleObject.getLastBlockBuffer() != null && OssSingleObject.getLastBlockBuffer().length != 0) {
                        byte[] byteBuffer = OssSingleObject.getLastBlockBuffer();
                        LOG.info("post writer single object last merge block size is : {}", byteBuffer.length);
                        this.ossWriterProxy.uploadOnePartForSingleObject(byteBuffer, OssSingleObject.uploadId,
                                OssSingleObject.allPartETags, this.object, this::getHeaderBytes);
                    }

                    if (OssSingleObject.allPartETags.size() == 0) {
                        LOG.warn("allPartETags size is 0, there is no part of data need to be complete uploaded, "
                                + "skip complete multipart upload!");
                        this.ossWriterProxy.abortMultipartUpload(this.object,OssSingleObject.uploadId);
                        return;
                    }

                    *//**2. 完成complete upload *//*
                    LOG.info("begin complete multi part upload, bucket:{}, object:{}, uploadId:{}, all has upload part size:{}",
                            this.bucket, this.object, OssSingleObject.uploadId, OssSingleObject.allPartETags.size());
                    orderPartETages(OssSingleObject.allPartETags);
                    CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                            this.bucket, this.object, OssSingleObject.uploadId, OssSingleObject.allPartETags);
                    CompleteMultipartUploadResult completeMultipartUploadResult = this.ossWriterProxy.completeMultipartUpload(completeMultipartUploadRequest);
                    LOG.info(String.format("post final object etag is:[%s]", completeMultipartUploadResult.getETag()));
                } catch (Exception e) {
                    LOG.error("osswriter post error: {}", e.getMessage(), e);
                    throw DataXException.asDataXException(e.getMessage());
                }
            }*/
        }

        @Override
        public void destroy() {
            try {
                this.minioClient.close();
            } catch (Exception e) {
                LOG.error("shutdown ossclient meet a exception:" + e.getMessage(), e);
            }
        }

        //--------------------------------------------------------------------------------------------------------------

        /**
         * 必填基础参数校验
         */
        private void basicValidateParameter(){
            this.writerSliceConfig.getNecessaryValue(MinIOConstant.ENDPOINT, MinIOWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(MinIOConstant.ACCESS_ID, MinIOWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(MinIOConstant.ACCESS_KEY, MinIOWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(MinIOConstant.BUCKET, MinIOWriterErrorCode.REQUIRED_VALUE);
        }

        /**
         * 其它参数校验
         */
        private void validateParameter() {
            // 是否加密写入MinIO
            this.writerSliceConfig.getBool(MinIOConstant.ENCRYPT);
            // 二进制文件校验
            if (this.isBinaryFile){
                BinaryFileWriterUtil.validateParameter(this.writerSliceConfig);
                return;
            }

            if (!this.isPeer2PeerCopyMode()) {
                // 非对等拷贝模式下必选
                this.writerSliceConfig.getNecessaryValue(MinIOConstant.OBJECT, MinIOWriterErrorCode.REQUIRED_VALUE);
            }

            // 压缩
            String compress = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.COMPRESS);
            if (StringUtils.isNotBlank(compress)) {
                // 不支持压缩
                String errorMessage = String.format("MinIO writes do not support compression for the moment. The compressed item %s does not work", compress);
                LOG.error(errorMessage);
                throw DataXException.asDataXException(MinIOWriterErrorCode.ILLEGAL_VALUE, errorMessage);

            }
            UnstructuredStorageWriterUtil.validateParameter(this.writerSliceConfig);
            LOG.info("writeSingleObject is: {}", this.writeSingleObject);
        }

        private boolean isPeer2PeerCopyMode() {
            return this.isBinaryFile
                    || com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.SYNC_MODE_VALUE_COPY
                    .equalsIgnoreCase(this.syncMode);
        }

        private String getObjectDir(String object) {
            String dir = null;
            if (StringUtils.isBlank(object)) {
                dir = "";
            } else {
                dir = object.trim();
                dir = dir.endsWith("/") ? dir : String.format("%s/", dir);
            }
            return dir;
        }

        /**
         * 多个 task 写单个 Object
         * @param mandatoryNumber
         * @return
         */
        private List<Configuration> doSplitForWriteSingleObject(int mandatoryNumber) {
            /*LOG.info("writeSingleObject is true, begin do split for write single object.");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String object = this.writerSliceConfig.getString(MinIOConstant.OBJECT);

            InitiateMultipartUploadRequest uploadRequest = this.ossWriterProxy.getInitiateMultipartUploadRequest(
                    object);

            InitiateMultipartUploadResult uploadResult;
            try {
                uploadResult = this.ossWriterProxy.initiateMultipartUpload(uploadRequest);
            } catch (Exception e) {
                LOG.error("initiateMultipartUpload error: {}", e.getMessage(), e);
                throw DataXException.asDataXException(e.getMessage());
            }
            *//**
             *
             * see: https://help.aliyun.com/document_detail/31993.html
             *//*
            // 如果需要写同一个object，需要保证使用同一个upload Id
            String uploadId = uploadResult.getUploadId();
            OssSingleObject.uploadId = uploadId;
            LOG.info("writeSingleObject use uploadId: {}", uploadId);

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();
                splitedTaskConfig.set(Key.OBJECT, object);
                splitedTaskConfig.set(Key.UPLOAD_ID, uploadId);
                writerSplitConfigs.add(splitedTaskConfig);
            }
            return writerSplitConfigs;*/
            return null;
        }

        /**
         * 多个 task 写多个 Object
         * @param mandatoryNumber
         * @return
         */
        private List<Configuration> doSplitForWriteMultiObject(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String bucket = this.writerSliceConfig.getString(MinIOConstant.BUCKET);
            String object = this.writerSliceConfig.getString(MinIOConstant.OBJECT);
            Set<String> allObjects = new HashSet<String>();
            // 第一步：查询所有 Object
            try {
                ListObjectsArgs objectsArgs = ListObjectsArgs.builder().bucket(bucket).recursive(true).build();
                Iterable<Result<Item>> results = minioClient.listObjects(objectsArgs);
                for (Result<Item> result : results) {
                    Item item = result.get();
                    allObjects.add(item.objectName());
                }
            } catch (Exception e) {
                LOG.warn("Error in list object: {}", e.getMessage());
                throw DataXException.asDataXException(MinIOWriterErrorCode.MinIO_COMM_ERROR, e.getMessage(), e);
            }
            // 第二步：添加后缀
            String objectSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration sliceConfig = this.writerSliceConfig.clone();
                // 添加uuid后缀
                objectSuffix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
                String fullObjectName = String.format("%s__%s", object, objectSuffix);
                while (allObjects.contains(fullObjectName)) {
                    // 冲突重置uuid后缀
                    objectSuffix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
                    fullObjectName = String.format("%s__%s", object, objectSuffix);
                }
                allObjects.add(fullObjectName);
                sliceConfig.set(MinIOConstant.OBJECT, fullObjectName);
                LOG.info(String.format("splited write object name:[%s]", fullObjectName));
                writerSplitConfigs.add(sliceConfig);
            }
            return writerSplitConfigs;
        }
    }

    /**
     * Task
     */
    public static class Task extends Writer.Task {
        @Override
        public void startWrite(RecordReceiver lineReceiver) {

        }

        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }
    }
}
