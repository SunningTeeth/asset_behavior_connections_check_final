package com.lanysec.services;

import com.lanysec.config.JavaKafkaConfigurer;
import com.lanysec.config.ModelParamsConfigurer;
import com.lanysec.utils.ConversionUtil;
import com.lanysec.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * @author daijb
 * @date 2021/3/8 10:49
 */
public class AssetBehaviorRelationCheck implements AssetBehaviorConstants {

    private static final Logger logger = LoggerFactory.getLogger(AssetBehaviorRelationCheck.class);

    public static void main(String[] args) {
        AssetBehaviorRelationCheck assetBehaviorRelationCheck = new AssetBehaviorRelationCheck();
        // 启动任务
        assetBehaviorRelationCheck.run(args);
    }

    public void run(String[] args) {
        logger.info("flink streaming is starting....");

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 重试4次，每次间隔20s
        streamExecutionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, Time.of(20, TimeUnit.SECONDS)));
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //加载kafka配置信息
        Properties properties = JavaKafkaConfigurer.getKafkaProperties(args);
        System.setProperty("mysql.servers", properties.getProperty("mysql.servers"));
        logger.info("load predefined properties : " + properties.toString());
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrap.servers"));
        //可根据实际拉取数据等设置此值，默认30s
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        //每次poll的最大数量
        //注意该值不要改得太大，如果poll太多数据，而不能在下次poll之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
        //当前消费实例所属的消费组
        //属于同一个组的消费实例，会负载消费消息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("group.id"));

        //启动定时任务
        startFunc();

        // 添加kafka source
        DataStream<String> kafkaSource = streamExecutionEnvironment.addSource(new FlinkKafkaConsumer010<>(properties.getProperty("topic"), new SimpleStringSchema(), props));

        /**
         * 过滤kafka无匹配资产的数据
         */
        SingleOutputStreamOperator<String> kafkaSourceFilter = kafkaSource.filter((FilterFunction<String>) value -> {
            JSONObject line = (JSONObject) JSONValue.parse(value);
            return !StringUtil.isEmpty(ConversionUtil.toString(line.get("SrcID")));
        });

        // 检测系列操作_new

        SingleOutputStreamOperator<String> checkStreamMap = kafkaSourceFilter.map(new CheckModelMapSourceFunction());
        SingleOutputStreamOperator<String> matchCheckStreamFilter = checkStreamMap.filter((FilterFunction<String>) value -> !StringUtil.isEmpty(value));

        // 将过滤数据送往kafka  topic
        String brokers = ConversionUtil.toString(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        String checkTopic = ConversionUtil.toString(props.getProperty("check.topic"));
        matchCheckStreamFilter.addSink(new FlinkKafkaProducer010<>(
                brokers,
                checkTopic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema())
        ));

        try {
            streamExecutionEnvironment.execute("kafka message streaming start ....");
        } catch (Exception e) {
            logger.error("flink streaming execute failed", e);

        }
    }

    private void startFunc() {
        logger.info("starting build model params.....");
        new Timer("timer-model").schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    ModelParamsConfigurer.reloadModelingParams();
                    ModelParamsConfigurer.reloadBuildModelResult();
                    logger.info("reload model params configurer.");
                } catch (Throwable throwable) {
                    logger.error("timer schedule at fixed rate failed ", throwable);
                }
            }
        }, 1000 * 10, 1000 * 60 * 5);
    }
}
