package com.gavin.gmallrealtime.gmallpublisher.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface PublisherService {
    // 获取总的日活
    Long getDau(String date);

    // 获取小时的日活
    Map<String, Long> getHourDau(String date);

    // 获取指定日期的销售总额
    Double getTotalAmount(String date);

    // 获取小时的销售额
    Map<String, Double> getHourAmount(String date);

    // 从es读数据, 返回给需要的数据Controller
    Map<String, Object> getSaleDetailAndAgg(String date, String keyword, String aggField,
                                            int aggSize, int startPage, int sizePerPage) throws IOException;

}
