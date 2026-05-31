package com.codzilla.sqlservice.SqlService.validator;

import java.util.List;
import java.util.Map;


public interface TaskValidator {


    boolean validate(List<Map<String, Object>> correctRows,
                     List<Map<String, Object>> userRows);

    default String failMessage(List<Map<String, Object>> correctRows,
                               List<Map<String, Object>> userRows) {
        return "Wrong answer";
    }
}