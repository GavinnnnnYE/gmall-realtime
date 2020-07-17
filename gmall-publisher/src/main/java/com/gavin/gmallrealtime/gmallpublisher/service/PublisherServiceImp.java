package com.gavin.gmallrealtime.gmallpublisher.service;

import com.gavin.gmallrealtime.gmallpublisher.mapper.DauMapper;
import com.gavin.gmallrealtime.gmallpublisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    DauMapper dau;

    @Override
    public Long getDau(String date) {
        return dau.getDau(date);
    }

/*
        数据层
        // List(Map("loghour": "10", count: 100), Map,.....)
        List<Map<String, Object>> getHourDau(String date);
        select LOGHOUR, count(*) COUNT from GMALL_DAU where LOGDATE=#{date } group by LOGHOUR


        //  Map("10"->100, "11"->200. "12"->100)
*/

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDau = dau.getHourDau(date);
        HashMap<String, Long> result = new HashMap<>();
        for (Map<String, Object> map : hourDau) {
            String key = map.get("LOGHOUR").toString();
            Long value = (Long) map.get("COUNT");
            result.put(key, value);
        }
        return result;
    }

    @Autowired
    OrderMapper order;

    @Override
    public Double getTotalAmount(String date) {
        Double result = order.getTotalAmount(date);
        return result == null ? 0 : result;
    }

    @Override
    public Map<String, Double> getHourAmount(String date) {
        List<Map<String, Object>> hourAmountList = order.getHourAmount(date);
        HashMap<String, Double> resultMap = new HashMap<>();
        for (Map<String, Object> map : hourAmountList) {
            String key = map.get("CREATE_HOUR").toString();
            Double value = ((BigDecimal) map.get("SUM")).doubleValue();
            resultMap.put(key, value);
        }
        return resultMap;
    }
}
