package com.echostreams.pulsar.jms.auth;

public abstract class CommonPulsarAuthParams {

    private String serviceUrl;
    private String tlsTrustCertsFilePath;

    public CommonPulsarAuthParams() {
    }

    public CommonPulsarAuthParams(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public CommonPulsarAuthParams(String serviceUrl, String tlsTrustCertsFilePath) {
        this.serviceUrl = serviceUrl;
        this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getTlsTrustCertsFilePath() {
        return tlsTrustCertsFilePath;
    }

    public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
    }
}
