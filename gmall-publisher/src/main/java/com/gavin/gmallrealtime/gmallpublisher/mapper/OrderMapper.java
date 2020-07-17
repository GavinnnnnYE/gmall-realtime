package com.gavin.gmallrealtime.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    //获取订单总的销售额
    Double getTotalAmount(String date);

    //获取每小时的销售额明细
    List<Map<String, Object>> getHourAmount(String date);
}
