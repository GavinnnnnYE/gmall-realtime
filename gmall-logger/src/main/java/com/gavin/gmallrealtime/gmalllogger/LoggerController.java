package com.gavin.gmallrealtime.gmalllogger;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gavin.gmallrealtime.constant.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


//@ResponseBody  //表示返回值是一个 字符串, 而不是 页面名
@RestController      // 等价于: @Controller + @ResponseBody
public class LoggerController {
    // http://localhost:8080/log?log=abc    => abc
    //    @GetMapping("/log")
    //    public String doLog(@RequestParam("log") String a){
    //        System.out.println(a);
    //        return "ok";
    //    }

    //LogUplodaer 中上传数据用的是 POST 请求， 所以要用PostMapping接受map请求
    @PostMapping("/log")
    public String doLog(String log){
        //上传的时候， 加了 "log=" 所以可以直接解析出来
        //System.out.println(log);

        // 1. 给日志加上时间
        log = addTS(log);

        // 2. 数据落盘 (搞离线会用到)
        saveToDisk(log);

        // 3. 把数据写入kafka， 需要写入倒topic
        sendToKafka(log);

        return "Received";
    }

    Logger logger = LoggerFactory.getLogger(LoggerController.class);

    //添加时间戳的方法
    private String addTS(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts",System.currentTimeMillis());
        return obj.toJSONString();
    }

    //保存日志到磁盘的方法
    private void saveToDisk(String log) {
        logger.info(log);
    }

    //把日志发送到kafka
    @Autowired
    KafkaTemplate kafka;

    public void sendToKafka(String log){
        if (log.contains("\"startup\"")){
            kafka.send(GmallConstant.TOPIC_STARTUP, log);
        }else {
            kafka.send(GmallConstant.TOPIC_EVENT, log);
        }
    }



}
