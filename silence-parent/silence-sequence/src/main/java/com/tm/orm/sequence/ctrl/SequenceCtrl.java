package com.tm.orm.sequence.ctrl;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author yudm
 * @Date 2021/6/5 14:18
 * @Desc
 */
@RestController
@RequestMapping("/silence/sequence")
public class SequenceCtrl {

    @PostMapping("/getId")
    public Integer getId() {
        return null;
    }
}
