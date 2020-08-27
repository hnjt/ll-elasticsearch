package com.ll.elasticsearch.service;

import com.ll.elasticsearch.domain.Car;

import java.util.List;

public interface CarService {

    List<Car> findAll();

    List<Car> findAllLike(String name,Integer age);

    Car saveOrUpdate(Car car);

    String delete(String id);

    Car get (String id);

    List<Car> search (Car car);
}
