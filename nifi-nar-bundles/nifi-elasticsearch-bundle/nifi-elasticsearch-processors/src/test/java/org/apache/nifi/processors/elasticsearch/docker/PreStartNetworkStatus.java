package org.apache.nifi.processors.elasticsearch.docker;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class PreStartNetworkStatus {
    private String networkName;
    private boolean existedBefore;

}
