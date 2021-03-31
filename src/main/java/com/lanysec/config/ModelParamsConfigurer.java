package com.lanysec.config;

import com.lanysec.services.AssetBehaviorConstants;
import com.lanysec.utils.ConversionUtil;
import com.lanysec.utils.DbConnectUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author daijb
 * @date 2021/3/27 15:34
 */
public class ModelParamsConfigurer implements AssetBehaviorConstants {

    private static final Logger logger = LoggerFactory.getLogger(ModelParamsConfigurer.class);

    private static Map<String, Object> modelingParams;

    /**
     * 返回建模参数
     *
     * @return 建模参数k-v
     */
    public static Map<String, Object> getModelingParams() {
        if (modelingParams == null) {
            modelingParams = reloadModelingParams();
        }
        return modelingParams;
    }

    /**
     * 从数据库获取建模参数
     */
    public static Map<String, Object> reloadModelingParams() {
        Connection connection = DbConnectUtil.getConnection();
        Map<String, Object> result = new HashMap<>(15 * 3 / 4);
        try {
            String sql = "select * from modeling_params" +
                    " where model_type=1 and model_child_type =3" +
                    " and model_switch = 1 and model_switch_2 =1";
            //" and modify_time < DATE_SUB( NOW(), INTERVAL 10 MINUTE );";
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            while (resultSet.next()) {
                result.put(MODEL_ID, resultSet.getString("id"));
                result.put(MODEL_TYPE, resultSet.getString("model_type"));
                result.put(MODEL_CHILD_TYPE, resultSet.getString("model_child_type"));
                result.put(MODEL_RATE_TIME_UNIT, resultSet.getString("model_rate_timeunit"));
                result.put(MODEL_RATE_TIME_UNIT_NUM, resultSet.getString("model_rate_timeunit_num"));
                result.put(MODEL_RESULT_SPAN, resultSet.getString("model_result_span"));
                result.put(MODEL_RESULT_TEMPLATE, resultSet.getString("model_result_template"));
                result.put(MODEL_CONFIDENCE_INTERVAL, resultSet.getString("model_confidence_interval"));
                result.put(MODEL_HISTORY_DATA_SPAN, resultSet.getString("model_history_data_span"));
                result.put(MODEL_UPDATE, resultSet.getString("model_update"));
                result.put(MODEL_SWITCH, resultSet.getString("model_switch"));
                result.put(MODEL_SWITCH_2, resultSet.getString("model_switch_2"));
                result.put(MODEL_ATTRS, resultSet.getString("model_alt_params"));
                result.put(MODEL_TASK_STATUS, resultSet.getString("model_task_status"));
                result.put(MODEL_MODIFY_TIME, resultSet.getString("modify_time"));
            }
        } catch (Throwable throwable) {
            logger.error("Get modeling parameters from the database error ", throwable);
        }
        logger.info("Get modeling parameters from the database : " + result.toString());
        modelingParams = result;
        return result;
    }

    public static volatile List<Map<String, Object>> modelResult;

    public static List<Map<String, Object>> getModelResult() throws Exception {
        if (modelResult == null) {
            modelResult = reloadBuildModelResult();
        }
        return modelResult;
    }

    public static List<Map<String, Object>> reloadBuildModelResult() throws SQLException {
        String sql = "SELECT " +
                "c.modeling_params_id,r.dst_ip_segment,r.src_ip,r.src_id,c.model_check_alt_params " +
                "FROM model_result_asset_behavior_relation r,model_check_params c " +
                "WHERE " +
                "r.modeling_params_id = c.modeling_params_id " +
                "AND c.model_type = 1 " +
                "AND c.model_child_type = 3 " +
                "AND c.model_check_switch = 1 ";
        // "AND c.modify_time > DATE_SUB( NOW(), INTERVAL 10 MINUTE );";
        List<Map<String, Object>> result = new ArrayList<>();
        Connection connection = DbConnectUtil.getConnection();
        if (connection == null) {
            return result;
        }
        ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<>(5);
            String modelingParamsId = ConversionUtil.toString(resultSet.getString("modeling_params_id"));
            String dstIpSegment = ConversionUtil.toString(resultSet.getString("dst_ip_segment"));
            String srcIp = ConversionUtil.toString(resultSet.getString("src_ip"));
            String srcId = ConversionUtil.toString(resultSet.getString("src_id"));
            String modelCheckAltParams = ConversionUtil.toString(resultSet.getString("model_check_alt_params"));
            map.put("modelingParamsId", modelingParamsId);
            map.put("dstIpSegment", dstIpSegment);
            map.put("srcIp", srcIp);
            map.put("srcId", srcId);
            map.put("modelCheckAltParams", modelCheckAltParams);
            result.add(map);
        }
        modelResult = result;
        return result;
    }

}
