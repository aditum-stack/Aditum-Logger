package com.ten.aditum.logger.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;

import com.ten.aditum.logger.entity.ErrorLog;

public interface ErrorLogDao {

    int insert(@Param("pojo") ErrorLog pojo);

    int insertList(@Param("pojos") List<ErrorLog> pojo);

    List<ErrorLog> select(@Param("pojo") ErrorLog pojo);

    int update(@Param("pojo") ErrorLog pojo);

}
