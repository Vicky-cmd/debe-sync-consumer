package com.infotrends.in.debesync.api.health;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class TaskMetrics {
    private long successCount;
    private long erroredCount;
}
