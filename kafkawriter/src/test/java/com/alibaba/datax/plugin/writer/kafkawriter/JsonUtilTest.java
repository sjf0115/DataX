package com.alibaba.datax.plugin.writer.kafkawriter;

import org.junit.Test;

import java.util.List;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/1/11 15:58
 */
public class JsonUtilTest {
    @Test
    public void testParseMappingConfig(){
       String jsonConfig = "[{\"name\":\"id\",\"type\":\"JSON_NUMBER\"}, {\"name\":\"name\",\"type\":\"JSON_STRING\"}, {\"name\":\"age\",\"type\":\"JSON_NUMBER\"}]";
       List<JsonMappingConfig> configs = JsonUtil.parseMappingConfig(jsonConfig);
       for (JsonMappingConfig config : configs) {
           String name = config.getName();
           JsonType type = config.getType();
           System.out.println(name + "," + type);
       }
    }

    @Test
    public void testConvertRecordToJsonString() {
    }
}
