package org.spring.springboot.kafka.producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * 城市 Controller 实现 Restful HTTP 服务
 *
 * Created by bysocket on 07/02/2017.
 */
@RestController
public class ProducerController {

	@Autowired
	private MsgProducer msgProducer;

    @RequestMapping(value = "/api", method = RequestMethod.GET)
    public void productSend() {
    	msgProducer.send();
    }

}