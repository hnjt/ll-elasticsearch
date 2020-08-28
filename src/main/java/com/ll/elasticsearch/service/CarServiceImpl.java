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


    @Override
    public List<Car> searchQuery(Car car,Integer page,Integer size) {

        //分页处理
        Pageable pageable = PageRequest.of(page, size);
        //检索条件
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        if(StringUtils.isNotBlank(car.getName()))
            bqb.must(QueryBuilders.matchPhraseQuery("name",car.getName()));//模糊匹配
        
        //filter 效率比 must高的多
        //bqb.filter(QueryBuilders.termQuery("routerDatabaseNo", query.getRouterDatabaseNo()));

        //时间段 一定要有头有尾 不然会出现慢查询
        //bqb.filter(QueryBuilders.rangeQuery("createTime").from( query.getCreateTime()).to(query.getUpdateTime()));
        
        //排序处理
        FieldSortBuilder fsb = SortBuilders.fieldSort("date").order( SortOrder.DESC);//日期倒序
        //聚合
        TermsAggregationBuilder tab = AggregationBuilders.terms("age").field("age");//年龄聚合
        //查询构建
        SearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(bqb)
                .withSort(fsb)
                .addAggregation(tab)
                .withPageable(pageable)
                .build();
        AggregatedPage<Car> search = (AggregatedPage)repository.search(query);
        long totalElements = search.getTotalElements();
        int totalPages = search.getTotalPages();
        System.out.println(totalElements);
        System.out.println(totalPages);
        List<Car> content = search.getContent();
        return content;
    }
}
