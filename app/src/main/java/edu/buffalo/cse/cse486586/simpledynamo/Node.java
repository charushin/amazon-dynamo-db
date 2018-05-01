package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by charushi on 4/22/18.
 */

public class Node {

    public String _id;
    public String port;
    public String hash;
    public String pred_port;
    public String succ_port1;
    public String succ_port2;
    public boolean status;

    public Node(){

    }

    public Node(String _id, String port, String hash, String pred_port, String succ_port1, String succ_port2, boolean status) {
        this._id = _id;
        this.port = port;
        this.hash = hash;
        this.pred_port = pred_port;
        this.succ_port1 = succ_port1;
        this.succ_port2 = succ_port2;
        this.status = status;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String getPred_port() {
        return pred_port;
    }

    public void setPred_port(String pred_port) {
        this.pred_port = pred_port;
    }

    public String getSucc_port1() {
        return succ_port1;
    }

    public void setSucc_port1(String succ_port1) {
        this.succ_port1 = succ_port1;
    }

    public String getSucc_port2() {
        return succ_port2;
    }

    public void setSucc_port2(String succ_port2) {
        this.succ_port2 = succ_port2;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Node{" +
                "_id='" + _id + '\'' +
                ", port='" + port + '\'' +
                ", hash='" + hash + '\'' +
                ", pred_port='" + pred_port + '\'' +
                ", succ_port1='" + succ_port1 + '\'' +
                ", succ_port2='" + succ_port2 + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        if (_id != null ? !_id.equals(node._id) : node._id != null) return false;
        if (port != null ? !port.equals(node.port) : node.port != null) return false;
        return hash != null ? hash.equals(node.hash) : node.hash == null;
    }

    @Override
    public int hashCode() {
        int result = _id != null ? _id.hashCode() : 0;
        result = 31 * result + (port != null ? port.hashCode() : 0);
        result = 31 * result + (hash != null ? hash.hashCode() : 0);
        return result;
    }
}
