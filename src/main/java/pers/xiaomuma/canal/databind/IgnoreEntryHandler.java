package pers.xiaomuma.canal.databind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class IgnoreEntryHandler implements EntryHandler<Map<String, String>> {

    private final Logger logger = LoggerFactory.getLogger(IgnoreEntryHandler.class);

    @Override
    public void insert(Map<String, String> map) {
        logger.info("新增 {}", map);
    }

    @Override
    public void update(Map<String, String> before, Map<String, String> after) {
        logger.info("修改 before {}", before);
        logger.info("修改 after {}", after);
    }

    @Override
    public void delete(Map<String, String> map) {
        logger.info("删除 {}", map);
    }
}
