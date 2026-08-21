package vn.motdev.kafkaoutbox.outbox;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /** Lấy theo id tăng dần để giữ thứ tự insert — quan trọng cho ordering per aggregate. */
    List<OutboxEvent> findTop20ByStatusOrderByIdAsc(OutboxEvent.Status status);
}
