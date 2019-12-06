package cn.itheima.bean;

public class TableBean {
    private String id;
    private int amount;
    private String pid;
    private String name;
    private String flag;

    public TableBean() {
    }

    public TableBean(String id, int amount, String pid, String name, String flag) {
        this.id = id;

        this.amount = amount;
        this.pid = pid;
        this.name = name;
        this.flag = flag;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
