package com.alibaba.datax.plugin.writer.miniowriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum MinIOWriterErrorCode implements ErrorCode {
    CONFIG_INVALID_EXCEPTION("MinIOWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("MinIOWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("MinIOWriter-02", "您填写的参数值不合法."),
    Write_OBJECT_ERROR("MinIOWriter-03", "您配置的目标Object在写入时异常."),
    MinIO_COMM_ERROR("MinIOWriter-05", "执行相应的MinIO操作异常."),
    ;

    private final String code;
    private final String description;

    private MinIOWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }

}
