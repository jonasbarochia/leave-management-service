package com.crest.employee.leave.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.exceptionhandler.CsvExceptionHandler;
import com.opencsv.exceptions.CsvException;

import lombok.Getter;

@Getter
public class CsvDataExceptionHandler implements CsvExceptionHandler {

  private List<String> errorRecords = new ArrayList<>();
  
  @Override
  public CsvException handleException(CsvException e) throws CsvException {
    errorRecords.add(StringUtils.join(Arrays.toString(e.getLine()), " ", e.getMessage()));
    return null;
  }

}
