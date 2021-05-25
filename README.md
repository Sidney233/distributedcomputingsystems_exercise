# Email Assignment

## 待完成:

* 请在DSPPCode.flink.email_assignment.impl中创建EmailAssignmentImpl, 继承EmailAssignment, 实现抽象方法.

## 题目描述:

* ECNU的教职工所使用的工作邮箱的格式为`用户名@邮箱后缀`。一般而言，每个教职工可以为工作邮箱申请多个用户名作为别名。然而，由于同一学院的教职工拥有相同的邮箱后缀。例如：数据科学与工程学院的教职工的邮箱后缀都为`dase.ecnu.edu.cn`。因此，在申请别名时有如下规定：
  
  - 同一学院内的别名不能发生重复（如不能同时存在两个用户都拥有`a_name@dase.ecnu.edu.cn`的邮箱地址）。
  
  - 不同学院间的别名可以发生重复（如可以同时存在两个用户分别拥有`a_name@dase.ecnu.edu.cn`和`a_name@cs.ecnu.edu.cn`的邮箱地址）。
  
  - 此外，别名长度的取值范围是[5,11]，且别名只能由英文字母、数字或下划线（_）组成。
  
  **现有一组顺序的别名申请和注销的请求序列，要求结合以上的别名申请规定得出各请求的最终完成状态（成功或者失败）。**
  
* 输入:
  答题时无需编写代码来解析输入的文本格式。需要实现的抽象方法的输入为`DataStream<Request>`类型。其中每个字段的含义如下:

    * type: 申请（APPLY）或注销（REVOKE）。
    * id: 发出请求用户的工号，每个工号只对应一个邮箱账户。
    * depart: 由于邮件系统设计的原因，部门通过两位代码表示，分别为`firstLevelCode`（一级部门代码，如`1`表示信息学部）、`secondLevelCode`（一级部门内的二级部门代码，如`3`表示数据学院）。
    * alias: 请求的别名。
  ```
    Request{type=APPLY, id=12345, depart=Department{firstLevelCode=1, secondLevelCode=1}, alias="*invalid"}
    Request{type=APPLY, id=22222, depart=Department{firstLevelCode=1, secondLevelCode=3}, alias="a_name"}
    Request{type=APPLY, id=25222, depart=Department{firstLevelCode=1, secondLevelCode=3}, alias="a_name"}
    Request{type=REVOKE, id=22222, depart=Department{firstLevelCode=1, secondLevelCode=3}, alias="a_name"}
    Request{type=APPLY, id=25222, depart=Department{firstLevelCode=1, secondLevelCode=3}, alias="b_name"}
  ```

* 输出:
  输出每个请求的完成状态，`SUCCESS`表示请求成功执行，`FAILURE`表示请求执行失败。
  ```
    FAILURE
    SUCCESS
    FAILURE
    SUCCESS
    SUCCESS
  ```

