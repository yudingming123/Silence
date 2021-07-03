package com.tm.orm.test.ctrl;

import com.tm.orm.test.user.TestSvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @Author yudm
 * @Date 2021/5/30 20:10
 * @Desc
 */
@RestController
public class TestCtrl {
    @Resource
    private TestSvc testSvc;

    @GetMapping("test")
    public void test() {
        testSvc.insert();
    }

    @GetMapping("/add1")
    public Long add1() {
        return testSvc.add1();
    }

    @GetMapping("/add2")
    public Long add2() {
        return testSvc.add2();
    }

    @GetMapping("/add11")
    public Long add11() {
        return testSvc.add11();
    }

    @GetMapping("/add22")
    public Long add22() {
        return testSvc.add22();
    }

    @GetMapping("/addList")
    public int addList() {
        return testSvc.addList();
    }


    @GetMapping("/dusu")
    public void dusu() throws Exception {
        testSvc.dusu();
    }

    @GetMapping("/stsTest")
    public void stsTest() throws Exception {
        testSvc.stsTest();
    }
}
