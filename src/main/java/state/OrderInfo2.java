package state;

public class OrderInfo2 {
  private Long orderId;
  private String orderDate;
  private String address;

  public OrderInfo2() {}

  public OrderInfo2(Long orderId, String orderDate, String address) {
    this.orderId = orderId;
    this.orderDate = orderDate;
    this.address = address;
  }

  public Long getOrderId() {
    return orderId;
  }

  public void setOrderId(Long orderId) {
    this.orderId = orderId;
  }

  public String getOrderDate() {
    return orderDate;
  }

  public void setOrderDate(String orderDate) {
    this.orderDate = orderDate;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  @Override
  public String toString() {
    return "OrderInfo2{" +
            "orderId=" + orderId +
            ", orderDate='" + orderDate + '\'' +
            ", address='" + address + '\'' +
            '}';
  }

  public static OrderInfo2 string2OrderInfo2(String line) {
    OrderInfo2 orderInfo2 = new OrderInfo2();
    if (line != null && line.length() > 0) {
      String[] split = line.split(",");
      orderInfo2.setOrderId(Long.parseLong(split[0]));
      orderInfo2.setOrderDate(split[1]);
      orderInfo2.setAddress(split[2]);
    }
    return orderInfo2;
  }
}
