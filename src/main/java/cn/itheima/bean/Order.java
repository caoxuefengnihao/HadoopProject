package cn.itheima.bean;
public class Order {
    private int id;
    private int pid;
    private int amount;

    public Order() {
    }

    public Order(int id, int pid, int amount) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;

    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }
    public void setAmount(int amount) {
        this.amount = amount;
    }
}
