package com.lij.service.impl.anno;

import com.lij.dao.IAccountDao;
import com.lij.dao.impl.AccountDaoImpl;
import com.lij.factory.BeanFactory;
import com.lij.service.IAccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;

/**
 * 账户的业务层实现
 *     <bean id="accountService" class="com.lij.service.impl.AccountServiceImpl"
 *     scpe="" init-method="" destory-method=">
 *      <property name="" value="" | ref="" ></property>
 *     </bean>
 * 用于创建对象
 *      和xml中编写<bean></bean>标签的作用相同
 *      @Component
 *          将当前累存入spring容器
 *          value:用于指定bean的id，默认值为当前类名且首字母小写[如果注解的属性名为value，且只有这一个属性，那么value是可以不写的]
 *      @Service：一般用在业务层
 *      @Controller：一般用在表现层
 *      @Repository：一般用在持久层
 *          以上三个注解的作用和属性与Component是一模一样的，但是它们是spring框架为了提供明确的三层使用的注解，使三层对象更加清晰
 *
 * 用于注入数据
 *      和xml中编写<bean>标签中的<property>的作用相同
 *      @Autowired:
 *          作用：自动按照类型注入，只要容器中有唯一的一个bean对象类型，和要注入的变量类型相匹配，就可以注入成功
 *                  如果ioc容器中没有任何bean的类型和要注入的变量类型匹配，则报错
 *                  如果ioc容器中有哥哥类型匹配时，根据（value值）名称 做进一步的匹配
 *          出现的位置：可以是方法上也可以是变量上
 *          细节：在使用注解注入时，set方法就不是必须的了
 *
 *      @Qualifier：
 *          作用：在按照类中注入的基础上再按照名称注入，它在给类成员注入时不能单独使用，但是在给方案参数注入时可以
 *          属性：value：用于指定注入bean的id
 *      @Resource
 *          作用：直接按照bean的id注入，它可以独立使用
 *          属性：name
 *      以上三个注解都只能注入其他bean类型的数据，而基本类型和String类型无法使用上述注解实现
 *      另外，集合类型的注解只能通过xml来实现
 *       @Value:
 *          作用：注入基本数据类型和 String 类型数据的
 *          属性：value：用于指定值，它可以使用spring中的EL表达式
 *          spEL的写法：${表达式}
 *
 * 用于改变作用范围的
 *      <bean>标签中的scope属性相同
 *      @Scope
 *          作用：用于指定bean的作用范围
 *          属性：
 *              value：指定范围的取值，常用取值：singleton，prototype
 * 用于改变生命周期
 *      <bean>标签中的init-method / destory-method 属性相同
 */
@Service( "accountService")

public class AccountServiceImpl implements IAccountService {
//    @Autowired
//    @Qualifier("accountDao")

    @Resource(name="accountDao")
    private IAccountDao accountDao;
    public void saveAccount() {
        accountDao.saveAccount();
        System.out.println("在 AccountServiceImpl 中账户保存了");
    }


}
