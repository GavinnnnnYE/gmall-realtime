package com.gavin.gmallrealtime.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.gavin.gmallrealtime.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    PublisherService service;

    /*
    http://localhost:8070/realtime-total?date=2020-07-17
    [{"id":"dau","name":"新增日活","value":1200},
    {"id":"new_mid","name":"新增设备","value":233 },
    {"id":"order_amount","name":"新增交易额","value":1000.2 }]
    */

    @GetMapping("/realtime-total")
    public String realtimeTotal(String date) {
        Long dau = service.getDau(date);
        Double totalAmount = service.getTotalAmount(date);

        // json字符串先用java的数据结构表示, 最后使用json序列化工具直接转成json字符串
        List<Map<String, String>> result = new ArrayList<>();

        Map<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", dau.toString());
        result.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        result.add(map2);

        //{"id":"order_amount","name":"新增交易额","value":1000.2 }
        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", totalAmount.toString());
        result.add(map3);

        return JSON.toJSONString(result);
    }

    /*
    http://localhost:8070/realtime-hour?id=dau&date=2020-07-17
    {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
    "today":{"12":38,"13":1233,"17":123,"19":688 }}
    */

    @GetMapping("/realtime-hour")
    public String realTimeHour(String id, String date) {
        if ("dau".equals(id)) {
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));
            Map<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);
        }else if ("order_amount".equals(id)){
            /*
            http://localhost:8070/realtime-hour?id=order_amount&date=2020-07-17
            {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
            "today":{"12":38,"13":1233,"17":123,"19":688 }}
            */
            Map<String, Double> today = service.getHourAmount(date);
            Map<String, Double> yesterday = service.getHourAmount(getYesterday(date));
            Map<String, Map<String, Double>> result = new HashMap<>();
            result.put("today",today);
            result.put("yesterday",yesterday);
            return JSON.toJSONString(result);
        } else{
            return null;
        }
    }

    private String getYesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }
}



