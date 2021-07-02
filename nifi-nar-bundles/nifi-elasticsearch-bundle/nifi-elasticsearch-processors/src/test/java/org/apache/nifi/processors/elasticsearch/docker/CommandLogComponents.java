package org.apache.nifi.processors.elasticsearch.docker;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@AllArgsConstructor
@Setter
@Getter
public class CommandLogComponents {
    private String log;
    private String errorLog;
}
