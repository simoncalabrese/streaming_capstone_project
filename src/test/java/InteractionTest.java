import java.io.Serializable;
import java.util.Date;

public class InteractionTest implements Serializable {
    private String date;
    private Long categoryId;
    private String ip;
    private String interactionType;

    public InteractionTest(String date, Long categoryId, String ip, String interactionType) {
        this.date = date;
        this.categoryId = categoryId;
        this.ip = ip;
        this.interactionType = interactionType;
    }

    public InteractionTest() {
    }

    public String getDate() {
        return date;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public String getIp() {
        return ip;
    }

    public String getInteractionType() {
        return interactionType;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setInteractionType(String interactionType) {
        this.interactionType = interactionType;
    }
}