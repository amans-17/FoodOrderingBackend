/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.exchanges;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

// TODO: CRIO_TASK_MODULE_RESTAURANTSAPI
//  Implement GetRestaurantsRequest.
//  Complete the class such that it is able to deserialize the incoming query params from
//  REST API clients.
//  For instance, if a REST client calls API
//  /qeats/v1/restaurants?latitude=28.4900591&longitude=77.536386&searchFor=tamil,
//  this class should be able to deserialize lat/long and optional searchFor from that.
@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class GetRestaurantsRequest {
  @NonNull
  private Double latitude;
  @NonNull
  private Double longitude;
  private String searchFor;
  
  public boolean isValidGeoLocation() {
    if (getLatitude() != null
        && getLatitude() >= -90.0D
        && getLatitude() <= 90.0D
        && getLongitude() != null
        && getLongitude() >= -180.0D
        && getLongitude() <= 180.0D) {
      return true;
    }
    return false;
  }

}

