package com.ll.elasticsearch.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ll.elasticsearch.domain.Car;
import com.ll.elasticsearch.service.CarService;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/car")
public class CarController {

    @Autowired
    private CarService service;

    @GetMapping("/all")
    public String all(){
        List<Car> all = service.findAll();
        JSONArray cars = new JSONArray();
        cars.addAll(all);
        return cars.toJSONString();
    }

    @GetMapping("/get")
    public String get(
            @RequestParam (defaultValue = "") String id
    ){
        Car car = service.get(id);
        JSONObject object = new JSONObject();
        object.put("data",car);
        return object.toJSONString();
    }

    @PostMapping("/saveOrUpdate")
    public String saveOrUpdate(
            @RequestParam (required = false) String id,
            @RequestParam (required = false) String name,
            @RequestParam (required = false) Integer age
    ){
        Car car = new Car(id, name, age, null);
        car = service.saveOrUpdate(car);
        JSONObject object = new JSONObject();
        object.put("data",car);
        return object.toJSONString();
    }

    @PostMapping("/delete")
    public String delete(
            @RequestParam  String id
    ){
        String deleteId = service.delete(id);
        JSONObject object = new JSONObject();
        object.put("data",deleteId);
        return object.toJSONString();
    }

    @GetMapping("/findAllLike")
    public String findAllLike(
            @RequestParam (required = false) String name,
            @RequestParam (required = false) Integer age
    ){
        List<Car> all = service.findAllLike(name,age);
        JSONArray cars = new JSONArray();
        cars.addAll(all);
        return cars.toJSONString();
    }

    @GetMapping("/searchQuery")
    public String searchQuery(
            @RequestParam (required = false) String name,
            @RequestParam (required = false) Integer age,
            @RequestParam (required = false) Integer page,
            @RequestParam (required = false) Integer size
    ){
        Car car = new Car();
        car.setName( name );
        car.setAge( age );
        List<Car> all = service.searchQuery(car,page,size);
        JSONArray cars = new JSONArray();
        cars.addAll(all);
        return cars.toJSONString();
    }
}
