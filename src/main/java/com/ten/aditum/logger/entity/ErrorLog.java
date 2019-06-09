package com.ten.aditum.logger.entity;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * ERROR日志表
 */
@Data
@Accessors(chain = true)
public class ErrorLog {
    /**
     * 主键ID Auto
     */
    private Integer id;
    /**
     * LOG TAG
     */
    private String logTag;
    /**
     * LOG信息
     */
    private String logMsg;
    /**
     * LOG时间
     */
    private String logTime;
    /**
     * 日志状态 0DEBUG 1INFO 2WARN 3ERROR
     */
    private Integer logStatus;
    /**
     * 删除标记
     */
    private Integer isDeleted;
}
