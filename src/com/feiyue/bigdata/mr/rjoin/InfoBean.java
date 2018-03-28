package com.feiyue.bigdata.mr.rjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements Writable{

    private int order_id;
    private String dateString;
    private String p_id;
    private int amount;
    private String pname;
    private int category_id;
    private float price;

    private String flag;

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }


    public InfoBean(){}


    public void set(int order_id,String dateString,String p_id,int amount,String pname,int category_id,float price,String flag){
        this.order_id = order_id;
        this.dateString = dateString;
        this.p_id = p_id;
        this.amount = amount;
        this.pname = pname;
        this.category_id = category_id;
        this.price = price;
        this.flag = flag;
    }

    //private int order_id;
    //private String dateString;
    //private String p_id;
    //private int amount;
    //private String pname;
    //private int category_id;
    //private float price;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeUTF(dateString);
        dataOutput.writeUTF(p_id);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeInt(category_id);
        dataOutput.writeFloat(price);
        dataOutput.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.order_id = dataInput.readInt();
        this.dateString = dataInput.readUTF();
        this.p_id = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.category_id = dataInput.readInt();
        this.price = dataInput.readFloat();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "order_id=" + order_id + ",dateString=" + dateString + ",p_id=" + p_id + ",amount=" + amount + ",pname=" + pname +
                ",category_id="+category_id + ",price=" + price + ",flag=" + flag;
    }
}
