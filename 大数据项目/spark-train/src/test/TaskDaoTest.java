import com.fengjr.sparkproject.dao.ITaskDAO;
import com.fengjr.sparkproject.dao.factory.DAOFactory;
import com.fengjr.sparkproject.domain.Task;


/**
 * @ClassName TaskDaoTest
 * @Description TODO 任务管理dao测试类
 * @Author jiang.li
 * @Date 2019-10-17 17:02
 * @Version 1.0
 */
public class TaskDaoTest {
    public static void main(String[] args) {
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        Task task = taskDao.findById(2L);
        System.out.println(task.getTaskName());
    }
}
