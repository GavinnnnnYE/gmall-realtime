package com.gavin.gmallrealtime.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //得到当日总日活
    Long getDau(String date);

    List<Map<String, Object>> getHourDau(String date);


    //应该一行一个Map
/*
     +----------+-----------+
     | LOGHOUR  | COUNT(1)  |
    +----------+-----------+
    | 14       | 19        |
    | 18       | 16        |
    +----------+-----------+
*/
}
