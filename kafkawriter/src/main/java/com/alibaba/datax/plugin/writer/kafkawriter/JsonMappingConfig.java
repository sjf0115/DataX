package com.alibaba.datax.plugin.writer.kafkawriter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 功能：Json映射配置
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/1/11 14:07
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JsonMappingConfig {
    private String name;
    private JsonType type;
}
