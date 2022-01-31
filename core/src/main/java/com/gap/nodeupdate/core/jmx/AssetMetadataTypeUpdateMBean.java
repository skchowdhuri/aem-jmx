package com.gap.nodeupdate.core.jmx;

public interface AssetMetadataTypeUpdateMBean {
    
    boolean isAuditThreadRunning();
    
    String getAuditThreadStatus();
    
    void startAuditThread(String rootPath, String adminPassword, boolean purge);
    
    void stopAuditThread();

}