package exercise_3;

import java.io.Serializable;

public class Info implements Serializable {
    private Integer weight;
    private String path;

    public Info(){
        weight = Integer.MAX_VALUE;
        path = "";
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
