package cn.itheima.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class FriendsBean implements Writable {
    private String  username;
    private String friends;
    public FriendsBean() {
    }
    public FriendsBean(String username, String friends) {
        this.username = username;
        this.friends = friends;
    }
    public String getFriends() {
        return friends;
    }
    public void setFriends(String friends) {
        this.friends = friends;
    }
    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(username);
        dataOutput.writeBytes(friends);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        username = dataInput.readLine();
        friends = dataInput.readLine();
    }

    @Override
    public String toString() {
        return "FriendsBean{" +
                "username='" + username + '\'' +
                ", friends='" + friends + '\'' +
                '}';
    }
}
