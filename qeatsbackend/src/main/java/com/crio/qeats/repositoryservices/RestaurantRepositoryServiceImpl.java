/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositoryservices;

import ch.hsr.geohash.GeoHash;
import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.globals.GlobalConstants;
import com.crio.qeats.models.ItemEntity;
import com.crio.qeats.models.MenuEntity;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositories.ItemRepository;
import com.crio.qeats.repositories.MenuRepository;
import com.crio.qeats.repositories.RestaurantRepository;
import com.crio.qeats.utils.GeoLocation;
import com.crio.qeats.utils.GeoUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Provider;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;


@Service("restaurantRepositoryServiceImpl")
public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {

  @Autowired
  private RestaurantRepository restaurantRepository;

  @Autowired
  private ItemRepository itemRepository;

  @Autowired
  private MenuRepository menuRepository;

  @Autowired
  private RedisConfiguration redisConfiguration;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private Provider<ModelMapper> modelMapperProvider;

  
  private boolean isOpenNow(LocalTime time, RestaurantEntity re) {
    LocalTime openingTime = LocalTime.parse(re.getOpensAt());
    LocalTime closingTime = LocalTime.parse(re.getClosesAt());

    return time.isAfter(openingTime) && time.isBefore(closingTime);
  }

  String getGeoKey(Double latitude, Double longitude) {
    return GeoHash.withCharacterPrecision(latitude, longitude, 7).toBase32();
  }

  private boolean isRestaurantCloseByAndOpen(RestaurantEntity restaurantEntity,
      LocalTime currentTime, Double latitude, Double longitude, Double servingRadiusInKms) {
    return isOpenNow(currentTime, restaurantEntity)
      && 
      GeoUtils.findDistanceInKm(latitude, longitude,
          restaurantEntity.getLatitude(), restaurantEntity.getLongitude())
          < servingRadiusInKms;
  }

  public List<Restaurant> findAllRestaurantsCloseBy(Double latitude, Double longitude, 
      LocalTime currentTime, Double servingRadiusInKms) {
    List<Restaurant> restaurants = null;
    String key = getGeoKey(latitude, longitude);
    if (redisConfiguration != null && redisConfiguration.isCacheAvailable()) {
      Jedis jedis = redisConfiguration.getJedisPool().getResource();
      String encodedRests =  jedis.get(key);
      jedis.close();
      if (encodedRests != null) {
        restaurants = findAllRestaurantsCloseByFromCache(encodedRests, currentTime);
        return restaurants;
      }
    }
    if (redisConfiguration == null || !redisConfiguration.isCacheAvailable()) {
      redisConfiguration.initCache();
    }
    restaurants = findAllRestaurantsCloseFromDb(latitude, longitude,
    currentTime, servingRadiusInKms);

    
    return restaurants;
  }


  private List<Restaurant> findAllRestaurantsCloseFromDb(Double latitude,
      Double longitude,
      LocalTime currentTime, Double servingRadiusInKms) {

    List<RestaurantEntity> restaurantEntityList = restaurantRepository.findAll();
    ModelMapper modelMapper = modelMapperProvider.get();
    ObjectMapper objectMapper = new ObjectMapper();
    List<Restaurant> restaurantList = new ArrayList<>();
    for (RestaurantEntity restaurantEntity : restaurantEntityList) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime,
          latitude, longitude, servingRadiusInKms)) {
        if (isOpenNow(currentTime, restaurantEntity)) {
          if (GeoUtils.findDistanceInKm(latitude, longitude,
              restaurantEntity.getLatitude(), restaurantEntity.getLongitude())
              < servingRadiusInKms) {
            restaurantList.add(modelMapper.map(restaurantEntity, Restaurant.class));
          }
        }
      }
    }
    try {
      String json = objectMapper.writeValueAsString(restaurantList);
      Jedis jedis = redisConfiguration.getJedisPool().getResource();
      jedis.set(getGeoKey(latitude, longitude), json);
    //  jedis.close();
    } catch (Exception e) {
      System.out.println("JSON Parse Exception IN SERIALISATION");
    }
    return restaurantList;
  }
  
  private List<Restaurant> findAllRestaurantsCloseByFromCache(String encodedRests, 
        LocalTime currentTime) {
    ObjectMapper objectMapper = new ObjectMapper();
    List<Restaurant> restaurants = null;
    try {
      System.out.println("HIT IN CACHE");
      CollectionType javaType = objectMapper.getTypeFactory()
              .constructCollectionType(List.class, Restaurant.class);
      restaurants = objectMapper.readValue(encodedRests, javaType);
      Iterator<Restaurant> itr = restaurants.iterator();
      while (itr.hasNext()) {
        Restaurant x = (Restaurant) itr.next();
        if (!(currentTime.isAfter(LocalTime.parse(x.getOpensAt())) 
            && currentTime.isBefore(LocalTime.parse(x.getClosesAt())))) {
          itr.remove();
        }
      }
      return restaurants;
    } catch (Exception e) {
      System.out.println("JSON Parse Exception IN DESERIALISATION");
    }
    return null;
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose names have an exact or partial match with the search query.
  @Override
  public List<Restaurant> findRestaurantsByName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    ModelMapper modelMapper = modelMapperProvider.get();
    Optional<List<RestaurantEntity>> exactOptrestaurantEntityList = restaurantRepository
        .findRestaurantsByNameExact(searchString);
    Optional<List<RestaurantEntity>> partialOptrestaurantEntityList = restaurantRepository
        .findRestaurantsByPartialName(searchString);
    List<Restaurant> restaurantList = new ArrayList<>();
    List<RestaurantEntity> restaurantEntityList = new ArrayList<RestaurantEntity>();
    if (exactOptrestaurantEntityList.isPresent()) {
      restaurantEntityList.addAll(exactOptrestaurantEntityList.get());
    }
    if (partialOptrestaurantEntityList.isPresent()) {
      restaurantEntityList.addAll(partialOptrestaurantEntityList.get());
    }
    for (RestaurantEntity restaurantEntity : restaurantEntityList) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime,
          latitude, longitude, servingRadiusInKms)) {
        restaurantList.add(modelMapper.map(restaurantEntity, Restaurant.class));
      }
    }
    return restaurantList;
  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose attributes (cuisines) intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByAttributes(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    ModelMapper modelMapper = modelMapperProvider.get();
    List<Restaurant> restaurantList = new ArrayList<>();
    Optional<List<RestaurantEntity>> optRestaurantEntityList = restaurantRepository
        .findRestaurantsByAttributes(searchString);

    if (optRestaurantEntityList.isPresent()) {
      List<RestaurantEntity> restaurantEntityList = optRestaurantEntityList.get();
      for (RestaurantEntity restaurantEntity : restaurantEntityList) {
        if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime,
              latitude, longitude, servingRadiusInKms)) {
          restaurantList.add(modelMapper.map(restaurantEntity, Restaurant.class));
        }
      }
    }
    return restaurantList;  
  }



  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose names form a complete or partial match
  // with the search query.

  @Override
  public List<Restaurant> findRestaurantsByItemName(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    ModelMapper modelMapper = modelMapperProvider.get();
    List<Restaurant> restaurants = new ArrayList<Restaurant>();
    Optional<List<String>> itemsIds = itemRepository.findItemIdsByName(searchString);
    if (itemsIds.isPresent()) {
      Optional<List<MenuEntity>> menus = menuRepository.findMenusByItemsItemIdIn(itemsIds.get());
      if (menus.isPresent()) {
        menus.get().forEach(e -> {
          Optional<RestaurantEntity> re = restaurantRepository
              .findRestaurantById(e.getRestaurantId());
          if (re.isPresent() && isRestaurantCloseByAndOpen(re.get(), currentTime,
              latitude, longitude, servingRadiusInKms)) {
            restaurants.add(modelMapper.map(re.get(), Restaurant.class));
          }
        });
      }
    }
    return restaurants;
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose attributes intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByItemAttributes(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    ModelMapper modelMapper = modelMapperProvider.get();
    List<Restaurant> restaurants = new ArrayList<Restaurant>();
    Optional<List<String>> itemsIds = itemRepository.findItemIdsByAttributes(searchString);
    if (itemsIds.isPresent()) {
      Optional<List<MenuEntity>> menus = menuRepository.findMenusByItemsItemIdIn(itemsIds.get());
      if (menus.isPresent()) {
        menus.get().forEach(e -> {
          Optional<RestaurantEntity> re = restaurantRepository
              .findRestaurantById(e.getRestaurantId());  
          if (re.isPresent() && isRestaurantCloseByAndOpen(re.get(), currentTime,
               latitude, longitude, servingRadiusInKms)) {
            restaurants.add(modelMapper.map(re.get(), Restaurant.class));
          }
        });
      }
    }
    return restaurants;  
  }

}

