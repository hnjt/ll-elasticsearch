package com.ll.elasticsearch.repository;

import com.ll.elasticsearch.domain.Car;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface CarRepository extends ElasticsearchRepository<Car,String> {

    Iterable<Car> findAllByNameIsLikeOrAgeBefore(String name,Integer age);
}
