package com.crio.qeats.utils;

import java.time.LocalTime;

import org.springframework.stereotype.Component;

public class PeakHoursUtils {
  LocalTime am8 = LocalTime.of(8, 00, 00);
  LocalTime am10 = LocalTime.of(10, 00, 00);
  LocalTime pm13 = LocalTime.of(13, 00, 00);
  LocalTime pm14 = LocalTime.of(14, 00, 00);
  LocalTime pm19 = LocalTime.of(19, 00, 00);
  LocalTime pm21 = LocalTime.of(21, 00, 00);

  public boolean isPeakHour(LocalTime currentTime) {
    if (inPeakInterval(am8, am10, currentTime)) { 
      return true;
    }
    if (inPeakInterval(pm13, pm14, currentTime)) { 
      return true;
    }
    if (inPeakInterval(pm19, pm21, currentTime)) { 
      return true;
    }
    return false;
  }

  private boolean inPeakInterval(LocalTime is, LocalTime ie, LocalTime time) {
    return time.compareTo(is) >= 0 && time.compareTo(ie) <= 0;
  }
}