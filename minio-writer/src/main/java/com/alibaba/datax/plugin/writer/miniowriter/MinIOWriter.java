package com.alibaba.datax.plugin.writer.miniowriter;


import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

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
        public void preCheck() {
            super.preCheck();
        }

        @Override
        public void init() {

        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return Collections.emptyList();
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {

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
