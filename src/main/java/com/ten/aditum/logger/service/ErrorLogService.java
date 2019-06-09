package com.ten.aditum.logger.service;

import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import com.ten.aditum.logger.entity.ErrorLog;
import com.ten.aditum.logger.mapper.ErrorLogDao;

@Service
public class ErrorLogService {

    @Resource
    private ErrorLogDao errorLogDao;

    public int insert(ErrorLog pojo) {
        return errorLogDao.insert(pojo);
    }

    public int insertList(List<ErrorLog> pojos) {
        return errorLogDao.insertList(pojos);
    }

    public List<ErrorLog> select(ErrorLog pojo) {
        return errorLogDao.select(pojo);
    }

    public int update(ErrorLog pojo) {
        return errorLogDao.update(pojo);
    }

}
