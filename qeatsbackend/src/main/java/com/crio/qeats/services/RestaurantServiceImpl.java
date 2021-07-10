
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import com.crio.qeats.utils.GeoUtils;
import com.crio.qeats.utils.PeakHoursUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ansi.AnsiStyle;
import org.springframework.stereotype.Service;

@Service("restaurantServiceImpl")
@Log4j2
@Data
public class RestaurantServiceImpl implements RestaurantService {

  private final PeakHoursUtils peakHoursUtil = new PeakHoursUtils();

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  
  @Resource(name = "restaurantRepositoryServiceImpl")
  private RestaurantRepositoryService restaurantRepositoryService;


  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    boolean isPeakHour = peakHoursUtil.isPeakHour(currentTime);
    Double servingRadiusInKms = isPeakHour
        ?
        peakHoursServingRadiusInKms : normalHoursServingRadiusInKms;
    List<Restaurant> readRests = restaurantRepositoryService.findAllRestaurantsCloseBy(
        getRestaurantsRequest.getLatitude(), 
        getRestaurantsRequest.getLongitude(), 
        currentTime, servingRadiusInKms);
    return new GetRestaurantsResponse(readRests); 
  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Implement findRestaurantsBySearchQuery. The request object has the search string.
  // We have to combine results from multiple sources:
  // 1. Restaurants by name (exact and inexact)
  // 2. Restaurants by cuisines (also called attributes)
  // 3. Restaurants by food items it serves
  // 4. Restaurants by food item attributes (spicy, sweet, etc)
  // Remember, a restaurant must be present only once in the resulting list.
  // Check RestaurantService.java file for the interface contract.

  private void addListToSet(List<Restaurant> list, LinkedHashSet<Restaurant> set) {
    if (list != null) {
      set.addAll((HashSet<Restaurant>)list.stream().collect(
          Collectors.toCollection(LinkedHashSet::new)));
    }
  }

  private void getUniquieResults(List<List<Restaurant>> llR, LinkedHashSet<Restaurant> set) {
    for (List<Restaurant> l: llR) {
      addListToSet(l, set);
    }
  }

  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    Double latitude = getRestaurantsRequest.getLatitude();
    Double longitude = getRestaurantsRequest.getLongitude();
    String searchString = getRestaurantsRequest.getSearchFor();
    
    if (searchString == null || searchString == "") {

      return new GetRestaurantsResponse(new ArrayList<Restaurant>()); 
    }
    boolean isPeakHour = peakHoursUtil.isPeakHour(currentTime);
    Double servingRadiusInKms = isPeakHour
        ?
        peakHoursServingRadiusInKms : normalHoursServingRadiusInKms;

    LinkedHashSet<Restaurant> uniqueRestaurants = new LinkedHashSet<Restaurant>();
    
    List<Restaurant> nameResult = restaurantRepositoryService
        .findRestaurantsByName(latitude, longitude, 
        searchString, currentTime, servingRadiusInKms);
    

    List<Restaurant> attributeResult = restaurantRepositoryService
        .findRestaurantsByAttributes(latitude, longitude, 
        searchString, currentTime, servingRadiusInKms);
    

    List<Restaurant> itemResult = restaurantRepositoryService
        .findRestaurantsByItemName(latitude, longitude, 
        searchString, currentTime, servingRadiusInKms);
    

    List<Restaurant> itemAttributeResult = restaurantRepositoryService
        .findRestaurantsByItemAttributes(latitude, longitude, 
        searchString, currentTime, servingRadiusInKms);

    getUniquieResults(Arrays.asList(nameResult, attributeResult, itemResult, itemAttributeResult),
        uniqueRestaurants);


    if (uniqueRestaurants.size() != 0) {
      return new GetRestaurantsResponse((ArrayList<Restaurant>)uniqueRestaurants.stream()
        .collect(Collectors.toList()));
    }
    return new GetRestaurantsResponse(new ArrayList<Restaurant>());
  }

  // TODO: CRIO_TASK_MODULE_MULTITHREADING
  // Implement multi-threaded version of RestaurantSearch.
  // Implement variant of findRestaurantsBySearchQuery which is at least 1.5x time faster than
  // findRestaurantsBySearchQuery.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQueryMt(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    Double latitude = getRestaurantsRequest.getLatitude();
    Double longitude = getRestaurantsRequest.getLongitude();
    String searchString = getRestaurantsRequest.getSearchFor();
        
    if (searchString == null || searchString == "") {
    
      return new GetRestaurantsResponse(new ArrayList<Restaurant>()); 
    }
    boolean isPeakHour = peakHoursUtil.isPeakHour(currentTime);
    Double servingRadiusInKms = isPeakHour
        ?
        peakHoursServingRadiusInKms : normalHoursServingRadiusInKms;
    
    LinkedHashSet<Restaurant> uniqueRestaurants = new LinkedHashSet<Restaurant>();

    CompletableFuture<List<Restaurant>> nameResult  
          = CompletableFuture.supplyAsync(() -> restaurantRepositoryService
          .findRestaurantsByName(latitude, longitude, 
          searchString, currentTime, servingRadiusInKms));
        
    CompletableFuture<List<Restaurant>> attributeResult
          = CompletableFuture.supplyAsync(() ->  restaurantRepositoryService
          .findRestaurantsByAttributes(latitude, longitude, 
          searchString, currentTime, servingRadiusInKms));
      
  
    CompletableFuture<List<Restaurant>> itemResult 
          = CompletableFuture.supplyAsync(() -> restaurantRepositoryService
          .findRestaurantsByItemName(latitude, longitude, 
          searchString, currentTime, servingRadiusInKms));
      
  
    CompletableFuture<List<Restaurant>> itemAttributeResult 
          = CompletableFuture.supplyAsync(() -> restaurantRepositoryService
          .findRestaurantsByItemAttributes(latitude, longitude, 
          searchString, currentTime, servingRadiusInKms));
  
    try {
      getUniquieResults(Arrays.asList(nameResult.get(), attributeResult.get(), 
          itemResult.get(), itemAttributeResult.get()),
          uniqueRestaurants);
    } catch (Exception e) {
      System.out.print("Error in future");
    }
    if (uniqueRestaurants.size() != 0) {
      return new GetRestaurantsResponse((ArrayList<Restaurant>)uniqueRestaurants.stream()
        .collect(Collectors.toList()));
    }
    return new GetRestaurantsResponse(new ArrayList<Restaurant>());    
  }
}

