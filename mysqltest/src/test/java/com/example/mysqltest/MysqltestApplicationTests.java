package com.example.mysqltest;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class MysqltestApplicationTests {

	@Test
	void contextLoads() {
	}
// 通过各种筛选条件查出的结果： teacherList （集合类型）
//Long count1 = teacherList.stream().filter(e -> e.getGender().equals(0)).count(); //男生数量
//Long count2 = teacherList.stream().filter(e -> e.getGender().equals(1)).count(); //女生数量
}
