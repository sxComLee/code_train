package dataStruct.tree.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName TreeNode
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-02-11 09:49
 * @Version 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TreeNode {
    String val;
    TreeNode leftNode;
    TreeNode rightNode;

}
