package spring.kafka.custom.partition.model;

public class Order {

    private String id;
    private String productName;
    private int quantity;
    private String ownerName;
    private int partition;

    public Order() {
    }

    public Order(String id, String productName, int quantity, String ownerName) {
        this.id = id;
        this.productName = productName;
        this.quantity = quantity;
        this.ownerName = ownerName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }
}
