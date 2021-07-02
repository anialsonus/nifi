package org.apache.nifi.processors.elasticsearch.docker;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class PreStartDockerNetworkParams {
    private String dockerNetworkName;
    private boolean dockerNetworkExistedBefore;
    private String dockerNetworkSubnet;
}
