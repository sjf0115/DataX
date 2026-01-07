package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * 功能：KafkaWriter
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2026/1/7 22:45
 */
public class KafkaWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        /**
         * Job对象初始化工作，此时可以通过super.getPluginJobConf()获取与本插件相关的配置。
         * 读插件获得配置中reader部分，写插件获得writer部分。
         */
        @Override
        public void init() {
            // 获取配置文件信息{parameter 里面的参数}
            this.conf = super.getPluginJobConf();
            log.info("kafka writer params:{}", conf.toJSON());
            // 参数校验
            this.validateParameter();
        }

        private void validateParameter() {
            // 必填项
            this.conf.getNecessaryValue(Key.TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(Key.BOOTSTRAP_SERVERS, KafkaWriterErrorCode.REQUIRED_VALUE);
        }

        /**
         * split: 拆分Task。参数adviceNumber框架建议的拆分数，一般是运行时所配置的并发度。值返回的是Task的配置列表。
         * @param mandatoryNumber 源端的切分数
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            // 为了做到 Reader、Writer 任务数对等，这里要求 Writer 插件必须按照源端的切分数进行切分。否则框架报错！
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.conf.clone());
            }
            return configurations;
        }

        @Override
        public void destroy() {
            // 不需要配置
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);
        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");
        private Producer<String, String> producer;
        private String fieldDelimiter;
        private Configuration conf;
        private List<String> columns;

        /**
         * init：Task对象的初始化。此时可以通过super.getPluginJobConf()获取与本Task相关的配置。这里的配置是Job的split方法返回的配置列表中的其中一个。
         */
        @Override
        public void init() {
            // 参数
            this.conf = super.getPluginJobConf();
            fieldDelimiter = conf.getUnnecessaryValue(Key.FIELD_DELIMITER, "\t", null);
            columns = conf.getList(Key.COLUMN, String.class);

            // 初始化 kafka
            Properties props = new Properties();
            props.put("bootstrap.servers", conf.getString(Key.BOOTSTRAP_SERVERS));
            props.put("acks", "all");//这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
            props.put("retries", 0);
            // Controls how much bytes sender would wait to batch up before publishing to Kafka.
            //控制发送者在发布到kafka之前等待批处理的字节数。
            //控制发送者在发布到kafka之前等待批处理的字节数。 满足batch.size和ling.ms之一，producer便开始发送消息
            //默认16384   16kb
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer(props);
        }

        /**
         * startWrite：从RecordReceiver中读取数据，写入目标数据源。RecordReceiver中的数据来自Reader和Writer之间的缓存队列。
         */
        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            log.info("start to writer kafka");
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                //说明还在读取数据,或者读取的数据没处理完
                //获取一行数据，按照指定分隔符 拼成字符串 发送出去
                producer.send(new ProducerRecord<String, String>(
                        this.conf.getString(Key.TOPIC),
                        recordToString(record), recordToString(record)));
            }
        }

        private String recordToString(Record record) {
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
            return sb.toString();
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
