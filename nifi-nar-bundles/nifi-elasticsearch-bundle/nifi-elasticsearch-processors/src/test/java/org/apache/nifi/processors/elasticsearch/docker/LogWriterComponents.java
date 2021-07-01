package org.apache.nifi.processors.elasticsearch.docker;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;

@AllArgsConstructor
@Setter
@Getter
public class LogWriterComponents {
    private Writer logWriter;
    private Writer errorLogWriter;
    private InputStream iStream;
    private InputStream errorIStream;
    private Reader  reader;
    private Reader errorReader;
}
