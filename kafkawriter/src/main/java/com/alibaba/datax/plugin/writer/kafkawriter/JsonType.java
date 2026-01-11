package com.alibaba.datax.plugin.writer.kafkawriter;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Objects;

/**
 * 功能：JSON 字段类型
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/1/11 14:07
 */
public enum JsonType {
    JSON_STRING(1, "JSON_STRING"),
    JSON_NUMBER(2, "JSON_NUMBER"),
    JSON_BOOL(3, "JSON_BOOL"),
    JSON_MAP(4, "JSON_MAP"),
    JSON_ARRAY(5, "JSON_ARRAY");

    private final int code;
    private final String value;

    JsonType(int code, String value) {
        this.code = code;
        this.value = value;
    }

    /*public static JsonType fromCode(int code) {
        for (JsonType type : values()) {
            if (Objects.equals(type.code, code)) {
                return type;
            }
        }
        return null;
    }

    public static JsonType fromValue(String value) {
        for (JsonType type : values()) {
            if (Objects.equals(type.value, value)) {
                return type;
            }
        }
        return null;
    }*/
}
