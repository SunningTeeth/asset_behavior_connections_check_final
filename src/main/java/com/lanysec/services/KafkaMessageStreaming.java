package com.lanysec.services;

import com.lanysec.config.JavaKafkaConfigurer;
import com.lanysec.config.ModelParamsConfigurer;
import com.lanysec.utils.ConversionUtil;
import com.lanysec.utils.DbConnectUtil;
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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * @author daijb
 * @date 2021/3/8 10:49
 */
public class KafkaMessageStreaming implements AssetBehaviorConstants {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageStreaming.class);

    public static void main(String[] args) {
        KafkaMessageStreaming kafkaMessageStreaming = new KafkaMessageStreaming();
        // 启动任务
        kafkaMessageStreaming.run(args);
    }

    public void run(String[] args) {
        logger.info("flink streaming is starting....");
        // 启动定时任务
        startFunc();
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 重试4次，每次间隔20s
        streamExecutionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, Time.of(20, TimeUnit.SECONDS)));
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //加载kafka配置信息
        Properties kafkaProperties = JavaKafkaConfigurer.getKafkaProperties(args);
        logger.info("load kafka properties : " + kafkaProperties.toString());
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        //可根据实际拉取数据等设置此值，默认30s
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        //每次poll的最大数量
        //注意该值不要改得太大，如果poll太多数据，而不能在下次poll之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
        //当前消费实例所属的消费组
        //属于同一个组的消费实例，会负载消费消息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty("group.id"));

        // 添加kafka source
        DataStream<String> kafkaSource = streamExecutionEnvironment.addSource(new FlinkKafkaConsumer010<>(kafkaProperties.getProperty("topic"), new SimpleStringSchema(), props));

        /**
         * 过滤kafka无匹配资产的数据
         */
        SingleOutputStreamOperator<String> kafkaSourceFilter = kafkaSource.filter((FilterFunction<String>) value -> {
            JSONObject line = (JSONObject) JSONValue.parse(value);
            if (StringUtil.isEmpty(ConversionUtil.toString(line.get("SrcID"))) ||
                    StringUtil.isEmpty(ConversionUtil.toString(line.get("DstID")))
            ) {
                return false;
            }
            return true;
        });

        // 检测系列操作_new
        {
            SingleOutputStreamOperator<String> checkStreamMap = kafkaSourceFilter.map(new CheckModelMapSourceFunction());
            SingleOutputStreamOperator<String> matchCheckStreamFilter = checkStreamMap.filter((FilterFunction<String>) value -> {
                if (StringUtil.isEmpty(value)) {
                    return false;
                }
                return true;
            });

            matchCheckStreamFilter.print().setParallelism(1);
            // 将过滤数据送往kafka csp_event topic
            String brokers = ConversionUtil.toString(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

            matchCheckStreamFilter.addSink(new FlinkKafkaProducer010<>(
                    brokers,
                    "csp_event",
                    new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema())
            ));
        }
        try {
            streamExecutionEnvironment.execute("kafka message streaming start ....");
        } catch (Exception e) {
            logger.error("flink streaming execute failed", e);
            // 更新状态
            updateModelTaskStatus(ModelStatus.STOP);
        }
    }

    /**
     * 解析kafka flow 检测数据
     */
    /*private static class ParserKafkaProcessFunction0 extends ProcessFunction<String, CheckFlowEntity> {

        @Override
        public void processElement(String value, Context ctx, Collector<CheckFlowEntity> out) throws Exception {
            CheckFlowEntity checkFlowEntity = JSON.parseObject(value, CheckFlowEntity.class);
            //输出到主流
            out.collect(checkFlowEntity);
        }
    }*/

    /**
     * 更新建模状态
     *
     * @param modelStatus 状态枚举
     */
    private void updateModelTaskStatus(ModelStatus modelStatus) {
        Object modelId = ModelParamsConfigurer.getModelingParams().get(MODEL_ID);
        String updateSql = "UPDATE `modeling_params` SET `model_task_status`=?, `modify_time`=? " +
                " WHERE (`id`='" + modelId + "');";
        DbConnectUtil.execUpdateTask(updateSql, modelStatus.toString().toLowerCase(), LocalDateTime.now().toString());
        logger.info("[kafkaMessageStreaming] update model task status : " + modelStatus.name());
    }


    private void startFunc() {
        logger.info("starting build model params.....");
        new Timer("timer-model").schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    ModelParamsConfigurer.reloadModelingParams();
                    logger.info("reload model params configurer.");
                } catch (Throwable throwable) {
                    logger.error("timer schedule at fixed rate failed ", throwable);
                }
            }
        }, 1000 * 10, 1000 * 60 * 5);

        new Timer("timer-model").schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    ModelParamsConfigurer.queryLastBuildModelResult();
                    logger.info("reload build model result.");
                } catch (Throwable throwable) {
                    logger.error("timer schedule at fixed rate failed ", throwable);
                }
            }
        }, 1000 * 60 * 5, 1000 * 60 * 30);
    }

}
