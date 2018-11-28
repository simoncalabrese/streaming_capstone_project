package capstone.bean.stream;

import java.io.Serializable;

public class Interaction implements Serializable {
    private static final long serialVersionUID = 2462824604393290326L;
    private Long date;
    private Long categoryId;
    private String ip;
    private String interactionType;

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getInteractionType() {
        return interactionType;
    }

    public void setInteractionType(String interactionType) {
        this.interactionType = interactionType;
    }
}