package com.echostreams.pulsar.jms.auth;

public class AthenzAuthParams extends CommonPulsarAuthParams {

    private String tenantDomain;
    private String tenantService;
    private String providerDomain;
    private String privateKey;
    private String keyId;

    public AthenzAuthParams() {
    }

    public AthenzAuthParams(String serviceUrl, String tlsTrustCertsFilePath, String tenantDomain, String tenantService, String providerDomain, String privateKey, String keyId) {
        super(serviceUrl, tlsTrustCertsFilePath);
        this.tenantDomain = tenantDomain;
        this.tenantService = tenantService;
        this.providerDomain = providerDomain;
        this.privateKey = privateKey;
        this.keyId = keyId;
    }

    public String getTenantDomain() {
        return tenantDomain;
    }

    public void setTenantDomain(String tenantDomain) {
        this.tenantDomain = tenantDomain;
    }

    public String getTenantService() {
        return tenantService;
    }

    public void setTenantService(String tenantService) {
        this.tenantService = tenantService;
    }

    public String getProviderDomain() {
        return providerDomain;
    }

    public void setProviderDomain(String providerDomain) {
        this.providerDomain = providerDomain;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public String getKeyId() {
        return keyId;
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }
}
