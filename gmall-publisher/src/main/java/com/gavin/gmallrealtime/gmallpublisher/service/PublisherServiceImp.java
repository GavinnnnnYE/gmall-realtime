package com.gavin.gmallrealtime.gmallpublisher.service;

import com.gavin.gmallrealtime.gmallpublisher.mapper.DauMapper;
import com.gavin.gmallrealtime.gmallpublisher.mapper.OrderMapper;
import com.gavin.gmallrealtime.gmallpublisher.util.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
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
/*
  "total": 200,
  "agg": Map("M"->100, "F" -> 100),
  "detail": List(Map(一样记录), Map(...))
*/
    @Override
    public Map<String, Object> getSaleDetailAndAgg(String date, String keyword, String aggField,
                                                   int aggSize, int startPage, int sizePerPage) throws IOException {
        //获取客户端
        JestClient client = ESUtil.getESClient();
        //生成DSL语句
        String dsl = ESUtil.getDSL(date, keyword, aggField, aggSize, startPage, sizePerPage);
        //创建查询对象，并把DSL语句传进去
        Search search = new Search.Builder(dsl)
                .addIndex("gmall_sale_detail")
                .addType("_doc")
                .build();
        SearchResult searchResult = client.execute(search);

        //解析结果
        //搞个Map用来存
        Map<String, Object> result = new HashMap<>();
        //1. 先把total获取到，直接用它自带的方法就行，ezy
        Long total = searchResult.getTotal();
        result.put("total", total);
        //2. 把detail拿到，放到列表里面，agg比较复杂，放后面搞
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);//传一个HashMap表示每单个Hit用一个map表示
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            detail.add(hit.source);
        }
        result.put("detail", detail);
        // 3. 获取聚合结果
        //搞个Map来存 => Map("M" -> 50, "F" -> 50)
        HashMap<String, Long> agg = new HashMap<>();
        List<TermsAggregation.Entry> buckets = searchResult.getAggregations()
                .getTermsAggregation("group_by_" + aggField)  //聚合字段
                .getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            String key = bucket.getKey();
            Long value = bucket.getCount();
            agg.put(key, value);
        }
        result.put("agg",agg);
        //把需要数据封装到Map中返回
        return result;
    }
}