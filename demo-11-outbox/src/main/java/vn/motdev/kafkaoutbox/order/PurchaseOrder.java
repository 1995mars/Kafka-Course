package vn.motdev.kafkaoutbox.order;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Dữ liệu NGHIỆP VỤ — thứ mà transaction phải bảo vệ cùng với event trong outbox.
 * (Đặt tên PurchaseOrder vì "ORDER" là từ khóa SQL.)
 */
@Entity
@Table(name = "purchase_orders")
public class PurchaseOrder {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String product;
    private int quantity;
    private Instant createdAt = Instant.now();

    protected PurchaseOrder() {
    }

    public PurchaseOrder(String product, int quantity) {
        this.product = product;
        this.quantity = quantity;
    }

    public Long getId() {
        return id;
    }

    public String getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
