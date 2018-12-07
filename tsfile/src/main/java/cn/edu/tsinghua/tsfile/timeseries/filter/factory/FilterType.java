package cn.edu.tsinghua.tsfile.timeseries.filter.factory;


public enum FilterType {
    VALUE_FILTER("value"), TIME_FILTER("time");

    private String name;
    FilterType(String name){
        this.name = name;
    }

    public String toString(){
        return name;
    }

}
