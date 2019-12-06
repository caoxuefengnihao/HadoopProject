package cn.itheima.bean;

public class Produce {
    private int pid;
    private String name;

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Produce() {
    }

    public Produce(int pid, String name) {
        this.pid = pid;
        this.name = name;
    }
}
