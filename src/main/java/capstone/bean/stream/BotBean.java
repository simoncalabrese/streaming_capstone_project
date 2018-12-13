package capstone.bean.stream;

import java.io.Serializable;

public class BotBean implements Serializable {
    private static final long serialVersionUID = 9201530948848440622L;

    private String ip;
    private Boolean ratio;
    private Boolean totRequests;
    private Boolean totCategories;
    private Double ratioAmount;
    private Long totRequestsAmont;
    private Long totCategoriesAmount;

    public BotBean(String ip, Boolean ratio, Boolean totRequests, Boolean totCategories, Double ratioAmount, Long totRequestsAmont, Long totCategoriesAmount) {
        this.ip = ip;
        this.ratio = ratio;
        this.totRequests = totRequests;
        this.totCategories = totCategories;
        this.ratioAmount = ratioAmount;
        this.totRequestsAmont = totRequestsAmont;
        this.totCategoriesAmount = totCategoriesAmount;
    }

    public BotBean() {
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Boolean getRatio() {
        return ratio;
    }

    public void setRatio(Boolean ratio) {
        this.ratio = ratio;
    }

    public Boolean getTotRequests() {
        return totRequests;
    }

    public void setTotRequests(Boolean totRequests) {
        this.totRequests = totRequests;
    }

    public Boolean getTotCategories() {
        return totCategories;
    }

    public void setTotCategories(Boolean totCategories) {
        this.totCategories = totCategories;
    }

    public Double getRatioAmount() {
        return ratioAmount;
    }

    public void setRatioAmount(Double ratioAmount) {
        this.ratioAmount = ratioAmount;
    }

    public Long getTotRequestsAmont() {
        return totRequestsAmont;
    }

    public void setTotRequestsAmont(Long totRequestsAmont) {
        this.totRequestsAmont = totRequestsAmont;
    }

    public Long getTotCategoriesAmount() {
        return totCategoriesAmount;
    }

    public void setTotCategoriesAmount(Long totCategoriesAmount) {
        this.totCategoriesAmount = totCategoriesAmount;
    }
}
