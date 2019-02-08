package com.echostreams.pulsar.jms.auth;

public class TLSAuthParams extends CommonPulsarAuthParams {
    private String tlsCertFile;
    private String tlsKeyFile;

    public TLSAuthParams() {
    }

    public TLSAuthParams(String serviceUrl, String tlsCertFile, String tlsKeyFile, String tlsTrustCertsFilePath) {
        super(serviceUrl, tlsTrustCertsFilePath);
        this.tlsCertFile = tlsCertFile;
        this.tlsKeyFile = tlsKeyFile;
    }

    public String getTlsCertFile() {
        return tlsCertFile;
    }

    public void setTlsCertFile(String tlsCertFile) {
        this.tlsCertFile = tlsCertFile;
    }

    public String getTlsKeyFile() {
        return tlsKeyFile;
    }

    public void setTlsKeyFile(String tlsKeyFile) {
        this.tlsKeyFile = tlsKeyFile;
    }

}
