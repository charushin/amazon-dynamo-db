package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by charushi on 4/22/18.
 */

public class Message {
    public String origin;
    public String key;
    public String value;
    public String sender;
    public String type;
    public String response;

    public Message(){

    }

    public Message(String origin, String key, String value, String sender, String type, String response) {
        this.origin = origin;
        this.key = key;
        this.value = value;
        this.sender = sender;
        this.type = type;
        this.response = response;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(origin+"#"+key+"#"+value+"#"+sender+"#"+type+"#"+response);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Message message = (Message) o;

        if (key != null ? !key.equals(message.key) : message.key != null) return false;
        return value != null ? value.equals(message.value) : message.value == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
