package com.pavan.analytics.consumer;

import com.pavan.analytics.service.EventAggregationService;
import com.pavan.common.event.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsEventConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(AnalyticsEventConsumer.class);

    private final EventAggregationService aggregationService;

    public AnalyticsEventConsumer(EventAggregationService aggregationService) {
        this.aggregationService = aggregationService;
    }

    @KafkaListener(id = "event-analytics-group", topics = "events")
    public void consume(EventMessage event) {
        log.debug("Received analytics event eventId={}, eventType={}",
                event.eventId(), event.eventType());
        aggregationService.aggregate(event);
        log.debug(
                "ANALYTICS EVENT AGGREGATED: eventId={}",
                event.eventId()
        );
    }
}
