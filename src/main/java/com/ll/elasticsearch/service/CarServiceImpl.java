package com.ll.elasticsearch.service;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ll.elasticsearch.domain.Car;
import com.ll.elasticsearch.repository.CarRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
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
}
