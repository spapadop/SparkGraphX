package exercise_3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Info implements Serializable {
    private Integer weight;
    private String path;

    public Info(){
        weight = Integer.MAX_VALUE;
        path = "";
    }

    public Info(int w, String p){
        this.weight = w;
        this.path = p;
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

    public String toString(){
        return this.getPath() + " " + this.getWeight();
    }

    public List<Long> getPathInLongs(){
        List<Long> pa = new ArrayList<>();
        String[] tokens = this.path.split(" ");
        for(String s: tokens){
            pa.add(Long.parseLong(s));
        }
        return pa;
    }
}
