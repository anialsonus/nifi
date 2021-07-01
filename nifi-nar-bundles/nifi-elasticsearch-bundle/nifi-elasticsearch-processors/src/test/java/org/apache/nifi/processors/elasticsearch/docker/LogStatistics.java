package org.apache.nifi.processors.elasticsearch.docker;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class LogStatistics {
    private String log;
    private String errorLog;

}
