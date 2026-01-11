package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaWriter {
    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        /**
         * init: Job对象初始化工作，此时可以通过super.getPluginJobConf()获取与本插件相关的配置。
         * 读插件获得配置中reader部分，写插件获得writer部分。
         */
        @Override
        public void init() {
            // 获取配置文件信息 {parameter 里面的参数}
            this.conf = super.getPluginJobConf();
            log.info("kafka writer params:{}", conf.toJSON());
            // 必填参数校验
            this.validateParameter();
        }

        /**
         * 必填参数校验
         */
        private void validateParameter() {
            this.conf.getNecessaryValue(Key.TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(Key.SERVER, KafkaWriterErrorCode.REQUIRED_VALUE);
        }

        /**
         * split: 拆分Task。参数 adviceNumber 为框架建议的拆分数，一般是运行时所配置的并发度。值返回的是 Task 的配置列表。
         * @param mandatoryNumber
         *  为了做到 Reader、Writer 任务数对等，这里要求 Writer 插件必须按照源端的切分数进行切分。否则框架报错！
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            // 按照 reader 配置文件的个数来组织相同个数的 writer 配置文件，确保 Reader、Writer 任务数相同
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        /**
         * post: 全局的后置工作。
         */
        @Override
        public void post() {
            log.info("job post");
        }

        /**
         * destroy: Job对象自身的销毁工作。
         */
        @Override
        public void destroy() {
            log.info("job destroy ");
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);
        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");
        private Producer<String, String> producer;
        private String fieldDelimiter;
        private String writeMode;
        private Integer keyIndex;
        private String nullKeyFormat;
        private Integer valueIndex;
        private String nullValueFormat;
        private String ack;
        private Integer batchSize;
        private String columns;
        private Configuration conf;

        /**
         * init：Task 对象的初始化。此时可以通过 super.getPluginJobConf() 获取与本 Task 相关的配置。
         * 这里的配置是 Job 的 split 方法返回的配置列表中的其中一个。
         */
        @Override
        public void init() {
            // 获取参数
            this.conf = super.getPluginJobConf();
            fieldDelimiter = conf.getUnnecessaryValue(Key.FIELD_DELIMITER, "\t", null);
            writeMode = conf.getUnnecessaryValue(Key.WRITE_MODE, "text", null);
            keyIndex = Integer.parseInt(conf.getString(Key.KEY_INDEX));
            nullKeyFormat = conf.getString(Key.NULL_KEY_FORMAT);
            valueIndex = Integer.parseInt(conf.getUnnecessaryValue(Key.VALUE_INDEX, null, null));
            nullValueFormat = conf.getString(Key.NULL_VALUE_FORMAT);
            ack = conf.getUnnecessaryValue(Key.ACK, "all", null);
            batchSize = Integer.parseInt(conf.getUnnecessaryValue(Key.BATCH_SIZE, "1024", null));
            columns = conf.getString(Key.COLUMN);

            log.info("task init params, fieldDelimiter: {}, writeMode: {}, keyIndex: {}, nullKeyFormat: {}, valueIndex: {}, nullValueFormat: {}, ack: {}, batchSize: {}, columns: {}",
                    fieldDelimiter, writeMode, keyIndex, nullKeyFormat, valueIndex, nullValueFormat, ack, batchSize, columns);

            // 参数校验
            validateParameter();

            // 初始化kafka
            Properties props = new Properties();
            props.put("bootstrap.servers", conf.getString(Key.SERVER));
            // 确认所有副本写入成功
            props.put("acks", ack);
            props.put("retries", 0);
            props.put("batch.size", batchSize);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
        }

        // 参数校验
        private void validateParameter() {
            // writeMode 如果配置，只能是 text 或者 json
            writeMode = writeMode.trim();
            Set<String> supportedWriteModes = Sets.newHashSet(Key.TEXT, Key.JSON);
            if (!supportedWriteModes.contains(writeMode)) {
                throw DataXException.asDataXException(
                        KafkaWriterErrorCode.ILLEGAL_VALUE,
                        String.format("writeMode only supports text and json modes, does not support the writeMode mode you configured: %s", writeMode));
            }
            ack = ack.trim();
            Set<String> supportedAck = Sets.newHashSet(Key.ALL, "0", "1");
            if (!supportedAck.contains(ack)) {
                throw DataXException.asDataXException(
                        KafkaWriterErrorCode.ILLEGAL_VALUE,
                        String.format("acks only supports 0、1 and all, does not support the acks mode you configured: %s", ack));
            }

            // keyIndex、valueIndex 校验 如果配置了，参数取值范围是大于等于0的整数
            if (keyIndex < 0) {
                throw DataXException.asDataXException(
                        KafkaWriterErrorCode.ILLEGAL_VALUE,
                        String.format("如果配置 keyIndex，参数取值范围必须是大于等于0的整数: %s", keyIndex));
            }
            if (valueIndex < 0) {
                throw DataXException.asDataXException(
                        KafkaWriterErrorCode.ILLEGAL_VALUE,
                        String.format("如果配置 valueIndex，参数取值范围必须是大于等于0的整数: %s", valueIndex));
            }

            // columns 当未配置 valueIndex，并且 writeMode 配置为 JSON 时必选
            if (Objects.equals(valueIndex, null) && Objects.equals(writeMode, Key.JSON)) {
                if (StringUtils.isBlank(columns)) {
                    throw DataXException.asDataXException(
                            KafkaWriterErrorCode.REQUIRED_VALUE,
                            "当未配置 valueIndex，并且 writeMode 配置为 JSON 时必选");
                }
            }
        }

        /**
         * prepare：局部的准备工作。
         */
        @Override
        public void prepare() {
            super.prepare();
        }

        /**
         * startWrite：从 RecordReceiver 中读取数据，写入目标数据源。RecordReceiver 中的数据来自 Reader 和 Writer 之间的缓存队列。
         */
        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            log.info("start to writer kafka");
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {// 说明还在读取数据,或者读取的数据没处理完
                // 获取一行数据，拼成指定字符串发送出去
                producer.send(new ProducerRecord<String, String>(
                        this.conf.getString(Key.TOPIC),
                        recordKey(record),
                        recordValue(record)));
            }
        }

        //  写入 Kafka 记录的 Key
        private String recordKey(Record record) {
            if (Objects.equals(keyIndex, null)) {
                // 如果不填写，写入 Kafka 记录 Key 为null
                return null;
            }
            int columnNumber = record.getColumnNumber();
            if (keyIndex >= columnNumber) {
                // 超过 Kafka 记录下标
                return null;
            }
            Column column = record.getColumn(keyIndex);
            String key = column.asString();
            if (Objects.equals(key, null)) {
                // keyIndex 指定的源端列值为 null 时，替换为该配置项指定的字符串。
                return nullKeyFormat;
            }
            return key;
        }

        // 写入 Kafka 记录的 Value
        private String recordValue(Record record) {
            // 2. 当未配置 valueIndex 时，使用 writeMode 配置项决定将源端读取记录的所有列拼接作为写入 Kafka 记录 Value 的格式，可选值为text和JSON，默认值为text。
            // 2.1 当配置 text 时，将所有列按照fieldDelimiter指定分隔符拼接
            // 2.2 当配置 json 时，将所有列按照 column 参数指定字段名称拼接为JSON字符串
            // 3. 当源端列值为 null 时，组装写入kafka记录Value时替换为 nullValueFormat 配置项指定的字符串，如果不配置不作替换。
            String value = null;
            if (!Objects.equals(valueIndex, null)) {
                // 当配置 valueIndex 时，读取对应列作为 Value
                Column column = record.getColumn(valueIndex);
                value = column.asString();
            } else if (Objects.equals(writeMode, Key.TEXT)) {
                // 当未配置 valueIndex 时，使用 writeMode 配置项决定将源端读取记录的所有列拼接作为写入 Kafka 记录 Value 的格式
                // 当配置 text 时，将所有列按照fieldDelimiter指定分隔符拼接
                int recordLength = record.getColumnNumber();
                if (0 == recordLength) {
                    return NEWLINE_FLAG;
                }
                Column column;
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < recordLength; i++) {
                    column = record.getColumn(i);
                    sb.append(column.asString()).append(fieldDelimiter);
                }
                sb.setLength(sb.length() - 1);
                sb.append(NEWLINE_FLAG);
                value = sb.toString();
            } else if (Objects.equals(writeMode, Key.JSON)) {
                // 当未配置 valueIndex 时，使用 writeMode 配置项决定将源端读取记录的所有列拼接作为写入 Kafka 记录 Value 的格式
                // 当配置 json 时，将所有列按照 column 参数指定字段名称拼接为JSON字符串
                List<JsonMappingConfig> jsonMappingConfigs = JsonUtil.parseMappingConfig(columns);
                return JsonUtil.convertRecordToJson(record, jsonMappingConfigs);
            }
            // 当源端列值为 null 时，组装写入 kafka 记录 Value 时替换为 nullValueFormat 配置项指定的字符串。
            if (Objects.equals(value, null)) {
                value = nullValueFormat;
            }
            return value;
        }

        /**
         * destroy: Task象自身的销毁工作。
         */
        @Override
        public void destroy() {
            log.info("Waiting for message to be successfully sent");
            producer.flush();
            log.info("Message sent successfully");
            if (producer != null) {
                producer.close();
            }
        }
    }
}