package com.gavin.gmallrealtime.gmallpublisher.controller;
import com.alibaba.fastjson.JSON;
import com.gavin.gmallrealtime.gmallpublisher.bean.Option;
import com.gavin.gmallrealtime.gmallpublisher.bean.SaleInfo;
import com.gavin.gmallrealtime.gmallpublisher.bean.Stat;
import com.gavin.gmallrealtime.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
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
        } else if ("order_amount".equals(id)) {
            /*
            http://localhost:8070/realtime-hour?id=order_amount&date=2020-07-17
            {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
            "today":{"12":38,"13":1233,"17":123,"19":688 }}
            */
            Map<String, Double> today = service.getHourAmount(date);
            Map<String, Double> yesterday = service.getHourAmount(getYesterday(date));
            Map<String, Map<String, Double>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);
        } else {
            return null;
        }
    }

    //返回昨天的年月日
    private String getYesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }

    //http://localhost:8070/sale_detail?date=2020-07-22&&startpage=1&&size=5&&keyword=手机小米
    @GetMapping("sale_detail")
    public String saleDetail(String date, String keyword, int size, int startpage) throws IOException {
        //调用service方法，把数据取到
        Map<String, Object> genderAgg =
                service.getSaleDetailAndAgg(date, keyword, "user_gender", 2, size, startpage);
        Map<String, Object> ageAgg =
                service.getSaleDetailAndAgg(date, keyword, "user_age", 100, size, startpage);

        // 1. 最终返回结果 ： （ 思路 => 用JavaBean封装，最后 JSON.toJSONString(SaleInfo)
        SaleInfo saleInfo = new SaleInfo();
        // 2. 设置总数
        saleInfo.setTotal((Long) genderAgg.get("total"));
        // 3. 设置详情
        saleInfo.setDetail((List<Map>) genderAgg.get("detail"));
        // 4. 添加饼图
        // 4.1 性别饼图
        Stat genderStat = new Stat();
        //4.1.1 给性别饼图设置title
        genderStat.setTitle("用户性别占比");
        //4.1.2 向性别饼图插入选项
        Map<String, Long> aggGender = (Map<String, Long>) genderAgg.get("agg");
        for (String key : aggGender.keySet()) {
            Long value = aggGender.get(key);
            Option option = new Option();
            option.setName(key);
            option.setValue(value);
            genderStat.addOption(option);
        }
        //添加性别饼图
        saleInfo.addStat(genderStat);

        // 4.2 年龄段的饼图
        Stat ageStat = new Stat();
        //  4.2.1 设置title
        ageStat.setTitle("用户年龄占比");
        // 4.2.2 插入选项
        ageStat.addOption(new Option("20岁以下", 0L));
        ageStat.addOption(new Option("20岁到30岁", 0L));
        ageStat.addOption(new Option("30岁及30岁以上", 0L));

        //把聚合的Map拿出来，这里是按年龄聚合的=> Map("18" -> 10, "20" -> 15, "30" -> 20 ...)
        //所以这里还要根据年龄聚合到年龄段
        Map<String, Long> aggAgeMap = (Map<String, Long>) ageAgg.get("agg");
        for (String key : aggAgeMap.keySet()) {
            int age = Integer.parseInt(key);
            if (age < 20){
                Option option = ageStat.getOptions().get(0);
                option.setValue(option.getValue() + aggAgeMap.get(key));
            }else if(age < 30){
                Option option = ageStat.getOptions().get(1);
                option.setValue(option.getValue() + aggAgeMap.get(key));
            }else{
                Option option = ageStat.getOptions().get(2);
                option.setValue(option.getValue() + aggAgeMap.get(key));
            }
        }

        //添加年龄饼图
        saleInfo.addStat(ageStat);
        return JSON.toJSONString(saleInfo);
    }
}
/*
 "total": 62,
 "stat": [{
     "options": [{
         "name": "20岁以下",
         "value": 30
         }, {
         "name": "20岁到30岁",
         "value": 80
         }, {
         "name": "30岁及30岁以上",
         "value": 70
         }],
     "title": "用户年龄占比"
         }, {
     "options": [{
         "name": "男",
         "value": 100
         }, {
         "name": "女",
         "value": 80
         }],
     "title": "用户性别占比"
         }],

 "detail": [{
     "user_id": "9",
     "sku_id": "8",
     .......,
     {.....},
     {.....},
     ......
     }]
     }
*/