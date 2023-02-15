package io.redpanda.examples;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonSerialize
public class TradeVO {

    private Long tradeId;

    private String ticketType;

    private String assetId;

    private String pfNumber;

    private BigDecimal parAmount;
}
