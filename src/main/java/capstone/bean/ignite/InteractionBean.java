package capstone.bean.ignite;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class InteractionBean {
    @QuerySqlField(index = true)
    private String ip;

    @QuerySqlField(index = true)
    private Long date;

    @QuerySqlField
    private Long categoryId;

    @QuerySqlField(index = true)
    private String interactionType;

    public InteractionBean(String ip, Long date, Long categoryId, String interactionType) {
        this.ip = ip;
        this.date = date;
        this.categoryId = categoryId;
        this.interactionType = interactionType;
    }

    public String getIp() {
        return ip;
    }

    public Long getDate() {
        return date;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public String getInteractionType() {
        return interactionType;
    }
}
