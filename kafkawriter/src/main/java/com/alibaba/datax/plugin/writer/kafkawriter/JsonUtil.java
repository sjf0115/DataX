package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Objects;

/**
 * 功能：JSON 工具
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/1/11 14:10
 */
public class JsonUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static List<JsonMappingConfig> parseMappingConfig(String jsonConfig) {
        try {
            return objectMapper.readValue(
                    jsonConfig,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, JsonMappingConfig.class)
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse mapping configuration", e);
        }
    }

    public static String convertRecordToJson(Record record, List<JsonMappingConfig> configs, String nullValueFormat) {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        // 循环读取字段映射配置
        // 当源端读取记录列的个数多于column配置的字段名个数时，写入时进行截断
        for (int i = 0; i < configs.size(); i++) {
            JsonMappingConfig config = configs.get(i);
            String fieldName = config.getName();
            JsonType fieldType = config.getType();
            Column column = null;
            if (i < record.getColumnNumber()) {
                column = record.getColumn(i);
            }
            setJsonValue(jsonNode, fieldName, fieldType, column, nullValueFormat);
        }

        try {
            return objectMapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert record to JSON", e);
        }
    }

    private static void setJsonValue(ObjectNode jsonNode, String fieldName, JsonType fieldType, Column column, String nullValueFormat) {
        // 当源端读取记录的列数少于column配置的字段名个数时，多余column配置字段名填充null或者nullValueFormat指定的字符串
        if (column == null) {
            jsonNode.set(fieldName, null);
            return;
        }

        // 拼接为JSON字符串
        switch (fieldType) {
            case JSON_STRING:
                handleStringType(jsonNode, fieldName, column.asString(), nullValueFormat);
                break;
            case JSON_NUMBER:
                handleNumberType(jsonNode, fieldName, column, nullValueFormat);
                break;
            case JSON_ARRAY:
                handleArrayType(jsonNode, fieldName, column.asString(), nullValueFormat);
            case JSON_MAP:
                handleMapType(jsonNode, fieldName, column.asString(), nullValueFormat);
            default:
                throw new IllegalArgumentException("Unsupported JSON type: " + fieldType);
        }
    }

    /**
     * 处理 JSON_STRING 类型
     */
    private static void handleStringType(ObjectNode jsonNode, String fieldName, String value, String nullValueFormat) {
        if (Objects.equals(value, null)) {
            jsonNode.put(fieldName, nullValueFormat);
        } else {
            jsonNode.put(fieldName, value);
        }
    }

    /**
     * 处理 JSON_NUMBER 类型
     */
    private static void handleNumberType(ObjectNode jsonNode, String fieldName, Column column, String nullValueFormat) {
        Column.Type columnType = column.getType();
        // 尝试将值转换为数字
        if (Objects.equals(column.asString(), null)) {
            jsonNode.put(fieldName, nullValueFormat);
        } else if (columnType == Column.Type.INT || columnType == Column.Type.LONG) {
            jsonNode.put(fieldName, column.asLong());
        } else if (columnType == Column.Type.DOUBLE) {
            jsonNode.put(fieldName, column.asDouble());
        } else {
            // 尝试解析字符串为数字
            try {
                String stringValue = column.asString().trim();
                if (stringValue.isEmpty()) {
                    jsonNode.set(fieldName, null);
                } else if (stringValue.contains(".") || stringValue.toLowerCase().contains("e")) {
                    jsonNode.put(fieldName, Double.parseDouble(stringValue));
                } else {
                    jsonNode.put(fieldName, Long.parseLong(stringValue));
                }
            } catch (NumberFormatException e) {
                // 如果无法转换为数字，则设为null
                jsonNode.set(fieldName, null);
            }
        }
    }

    /**
     * 处理 JSON_ARRAY 类型
     */
    private static void handleArrayType(ObjectNode jsonNode, String fieldName, String value, String nullValueFormat) {
        try {
            // 尝试解析JSON字符串为数组
            if (Objects.equals(value, null)) {
                jsonNode.put(fieldName, nullValueFormat);
            } else if (value.startsWith("[") && value.endsWith("]")) {
                ArrayNode arrayNode = (ArrayNode) objectMapper.readTree(value);
                jsonNode.set(fieldName, arrayNode);
            } else {
                // 如果不是有效的JSON数组字符串，设为null
                jsonNode.set(fieldName, null);
            }
        } catch (Exception e) {
            // JSON解析失败或其他异常，设为null
            jsonNode.set(fieldName, null);
        }
    }

    /**
     * 处理 JSON_MAP 类型
     */
    private static void handleMapType(ObjectNode jsonNode, String fieldName, String value, String nullValueFormat) {
        try {
            // 尝试解析JSON字符串为对象
            if (Objects.equals(value, null)) {
                jsonNode.put(fieldName, nullValueFormat);
            } else if (value.startsWith("{") && value.endsWith("}")) {
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(value);
                jsonNode.set(fieldName, objectNode);
            } else {
                // 如果不是有效的JSON对象字符串，设为null
                jsonNode.set(fieldName, null);
            }
        } catch (Exception e) {
            // JSON解析失败或其他异常，设为null
            jsonNode.set(fieldName, null);
        }
    }

    // 如果为 null 返回默认值
    private static Object defaultValue(Object o, String value) {
        if (Objects.equals(o, null)) {
            return value;
        }
        return o;
    }
}
