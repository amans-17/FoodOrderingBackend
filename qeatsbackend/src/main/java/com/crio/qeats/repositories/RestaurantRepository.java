/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositories;

import com.crio.qeats.models.RestaurantEntity;
import java.util.List;
import java.util.Optional;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

public interface RestaurantRepository extends MongoRepository<RestaurantEntity, String> {
  
  @Query("{attributes : ?0}")
  Optional<List<RestaurantEntity>> findRestaurantsByAttributes(String searchString);

  @Query("{name : ?0}")
  Optional<List<RestaurantEntity>> findRestaurantsByNameExact(String searchString);

  @Query("{name : { $regex : ?0}}")
  Optional<List<RestaurantEntity>> findRestaurantsByPartialName(String searchString);

  @Query("{id : ?0}")
  Optional<RestaurantEntity> findRestaurantById(String restaurantId);

}

