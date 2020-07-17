package com.gavin.gmallrealtime.gmallpublisher.service;

import java.util.List;
import java.util.Map;

public interface PublisherService {
    // 获取总的日活
    Long getDau(String date);

    Map<String, Long> getHourDau(String date);

    Double getTotalAmount(String date);

    Map<String, Double> getHourAmount(String date);
}
