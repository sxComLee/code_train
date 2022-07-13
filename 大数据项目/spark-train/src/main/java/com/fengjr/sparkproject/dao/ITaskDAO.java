package com.fengjr.sparkproject.dao;

import com.fengjr.sparkproject.domain.Task;

/**
 * @ClassName ITaskDao
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-10-17 10:22
 * @Version 1.0
 */
public interface ITaskDAO {
    /**
     * @Author jiang.li
     * @Description //TODO 根据主键查询任务
     * @Date 16:38 2019-10-17
     * @Param [taskid] 任务id
     * @return java.com.fengjr.sparkproject.domain.Task
     **/
    Task findById(Long taskid);

}
