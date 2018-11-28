package capstone.bean.ignite;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class BotBean {
    @QuerySqlField(index = true)
    private String ip;

    @QuerySqlField(index = true)
    private Long registerDate;

    public BotBean(String ip, Long registerDate) {
        this.ip = ip;
        this.registerDate = registerDate;
    }

    public String getIp() {
        return ip;
    }

    public Long getRegisterDate() {
        return registerDate;
    }
}
