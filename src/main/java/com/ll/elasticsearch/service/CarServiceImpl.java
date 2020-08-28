package com.ll.elasticsearch.service;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.ll.elasticsearch.domain.Car;
import com.ll.elasticsearch.repository.CarRepository;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class CarServiceImpl implements CarService{

    @Autowired
    private CarRepository repository;

    @Override
    public List<Car> findAll() {
        Iterable<Car> all = repository.findAll();
        Iterator<Car> it = all.iterator();
        ArrayList<Car> cars = new ArrayList<>();
        while (it.hasNext())
            cars.add(it.next());
        return cars;
    }

    @Override
    public Car saveOrUpdate(Car car) {
        if (StringUtils.isBlank(car.getId())){
            car.setId(UUID.randomUUID().toString().replace("-",""));
            car.setDate(new Date());
            return this.repository.save(car);
        }else {
            Car c = this.get(car.getId());
            this.repository.deleteById(car.getId());
            car.setDate(c.getDate());
            return this.repository.save(car);
        }
    }

    @Override
    public String delete(String id) {
        Car car = this.get(id);
        if (null != car && StringUtils.isNotBlank(car.getId())){
            this.repository.delete(car);
            return car.getId();
        }
        return null;
    }

    @Override
    public Car get(String id) {
        return this.repository.findById(id).orElse(new Car());
    }

    @Override
    public List<Car> search(Car car) {
        /*MinMaxPriorityQueue.Builder<>()
        this.repository.search()*/
        return null;
    }

    @Override
    public List<Car> findAllLike(String name,Integer age) {
        Iterable<Car> all = this.repository.findAllByNameIsLikeOrAgeBefore(name, age);
        Iterator<Car> it = all.iterator();
        ArrayList<Car> cars = new ArrayList<>();
        while (it.hasNext())
            cars.add(it.next());
        return cars;
    }

    ///**
    //     * 检索聚合查询，这里对taxonomy(学科分类进行聚合)
    //     * @param params
    //     * @return
    //     */
    //    @Override
    //    public SdbResult searchQuery(String q, String dataSetType, String taxonomy, String code, String username, boolean ordertime, boolean orderinfluence, String page, String size, Locale locale) {
    //
    //        Map<String, Object> map = Maps.newHashMap();
    //        Pageable pageable = PageRequest.of(Integer.parseInt(page), Integer.parseInt(size));
    //        //检索条件
    //        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
    //        if(StringUtils.isNotEmpty(dataSetType))
    //            bqb.must(QueryBuilders.matchPhraseQuery("dataSetType", dataSetType));
    //        if(StringUtils.isNotEmpty(code))
    //            bqb.must(QueryBuilders.matchPhraseQuery("code", code));
    //        if(StringUtils.isNotEmpty(taxonomy))
    //            bqb.must(QueryBuilders.matchPhraseQuery("taxonomy", taxonomy));
    //        if(StringUtils.isNotEmpty(q))
    //            bqb.must(QueryBuilders.multiMatchQuery(q, "title", "keyword", "introduction"));
    //        //排序条件
    //        FieldSortBuilder fsb = null;
    //        if(ordertime){
    //            fsb = SortBuilders.fieldSort("publishDate").order(SortOrder.DESC);
    //        }
    //        if(orderinfluence){
    //            fsb = SortBuilders.fieldSort("referenceNumber").order(SortOrder.DESC);
    //        }
    //		//聚合条件
    //        TermsAggregationBuilder builder1 = AggregationBuilders.terms("taxonomy").field("taxonomy.keyword");
    //        TermsAggregationBuilder builder2 = AggregationBuilders.terms("year").field("year.keyword");
    //        TermsAggregationBuilder builder = builder1.subAggregation(builder2);
    //        //构建查询
    //        SearchQuery query = new NativeSearchQueryBuilder()
    //                .withQuery(bqb)
    //                .withSort(fsb)
    //                .addAggregation(builder)
    //                .withPageable(pageable)
    //                .build();
    //        if ("en".equals(locale.getLanguage())) {
    //            AggregatedPage<DataSetIndexEn> search = (AggregatedPage)sdoIndexEnRepository.search(query);
    //            long totalElements = search.getTotalElements();
    //            int totalPages = search.getTotalPages();
    //            List<DataSetIndexEn> content = search.getContent();
    //			Terms term1 = (Terms)search.getAggregations().getAsMap().get("taxonomy");
    //            log.debug("term1============"+term1.toString());
    //            for (Terms.Bucket bucket : term1.getBuckets()) {
    //                log.debug("一级内容"+bucket.toString());
    //                map.put(bucket.getKey().toString(), bucket.getDocCount());
    //                Terms terms_year = bucket.getAggregations().get("year");
    //                for (Terms.Bucket year_bucket : terms_year.getBuckets()) {
    //                    log.debug("二级内容"+year_bucket.toString());
    //                    map.put(year_bucket.getKey().toString(), year_bucket.getDocCount());
    //                }
    //            }
    //            map.put("total",totalElements);
    //            map.put("totalPages",totalPages);
    //            map.put("recommendData",content);
    //        } else {
    //            AggregatedPage<DataSetIndexZh> search = (AggregatedPage)sdoIndexZhRepository.search(query);
    //            long totalElements = search.getTotalElements();
    //            int totalPages = search.getTotalPages();
    //            List<DataSetIndexZh> content = search.getContent();
    //			Terms term1 = (Terms)search.getAggregations().getAsMap().get("taxonomy");
    //            log.debug("term1============"+term1.toString());
    //            for (Terms.Bucket bucket : term1.getBuckets()) {
    //                log.debug("一级内容"+bucket.toString());
    //                map.put(bucket.getKey().toString(), bucket.getDocCount());
    //                Terms terms_year = bucket.getAggregations().get("year");
    //                for (Terms.Bucket year_bucket : terms_year.getBuckets()) {
    //                    log.debug("二级内容"+year_bucket.toString());
    //                    map.put(year_bucket.getKey().toString(), year_bucket.getDocCount());
    //                }
    //            }
    //            map.put("total",totalElements);
    //            map.put("totalPages",totalPages);
    //            map.put("recommendData",content);
    //        }
    //        return success(map);
    //    }

    @Override
    public List<Car> searchQuery(Car car,Integer page,Integer size) {

        //分页处理
        Pageable pageable = PageRequest.of(page, size);
        //检索条件
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        if(StringUtils.isNotBlank(car.getName()))
            bqb.must(QueryBuilders.matchPhraseQuery("name",car.getName()));
        //排序处理
        FieldSortBuilder fsb = null;
//        Pageable pageable = PageRequest.of(1, 8, Sort.Direction.DESC, "publishDate");
        fsb = SortBuilders.fieldSort("date").order( SortOrder.DESC);
        //聚合
//        TermsAggregationBuilder builder1 = AggregationBuilders.terms("age").field("taxonomy.keyword");
//        TermsAggregationBuilder builder2 = AggregationBuilders.terms("year").field("year.keyword");
//        TermsAggregationBuilder builder = builder1.subAggregation(builder2);
        //查询构建
        SearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(bqb)
                .withSort(fsb)
//                .addAggregation(builder1)
                .withPageable(pageable)
                .build();
        AggregatedPage<Car> search = (AggregatedPage)repository.search(query);
        List<Car> content = search.getContent();
        return content;
    }
}
