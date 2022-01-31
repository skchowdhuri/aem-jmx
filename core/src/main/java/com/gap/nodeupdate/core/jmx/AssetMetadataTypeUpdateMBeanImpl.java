package com.gap.nodeupdate.core.jmx;

import com.day.cq.commons.jcr.JcrConstants;
import com.day.cq.dam.api.DamConstants;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component(
        service = AssetMetadataTypeUpdateMBean.class,
        property = "jmx.objectname=com.gap.nodeupdate.core.jmx:type=AssetMetadataTypeUpdate,name=AssetMetadataTypeUpdate",
        immediate = true
        )
public class AssetMetadataTypeUpdateMBeanImpl implements AssetMetadataTypeUpdateMBean {
    
    private static final String PROPERTY_VALUE_UNPROCESSED = "unProcessed";
    private static final String PROPERTY_VALUE_PROCESSED = "processed";
    
	private static final String NODE_SUBASSETS = "subassets";

	private static final String PROPERTY_ASSETSTATE = "dam:assetState";
	private static final String PROPERTY_PATH_ASSETSTATE = JcrConstants.JCR_CONTENT + "/" + PROPERTY_ASSETSTATE;

	private static final Logger LOG = LoggerFactory.getLogger(AssetMetadataTypeUpdateMBean.class);

    @Reference
    public Repository repository;
    
    private AtomicBoolean auditThreadRunning = new AtomicBoolean(false);
    private AtomicBoolean auditThreadShouldQuit = new AtomicBoolean(false);
    
    private AtomicLong auditThreadNodeCount = new AtomicLong(0);

    private AtomicLong auditThreadAssetsSeen = new AtomicLong(0);
    private AtomicLong auditThreadAssetsFixed = new AtomicLong(0);
    
    private String status = "Idle";
    
    private ExecutorService executorService;

    private String defaultDateStr = "2022-01-31 20:23";
    private SimpleDateFormat curFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private Calendar calendar = Calendar.getInstance();

    @Activate
    public void activate(ComponentContext ctx) {
        LOG.info("activate: Activating SubAssetAssetStateAuditMBeanImpl");
        this.executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public boolean isAuditThreadRunning() {
        return auditThreadRunning.get();
    }

    @Override
    public String getAuditThreadStatus() {
        return status + ": [ " + isAuditThreadRunning() + 
                "; nodesSeen = " + auditThreadNodeCount.get() + 
                "; assetsSeen = " + auditThreadAssetsSeen.get() +
                "; assetsFixed = " + auditThreadAssetsFixed.get() +
        "]";

    }
    
    @Override
    public void startAuditThread(String path, String adminPassword, boolean fix) {

        if (auditThreadRunning.get()) {
            LOG.warn("startAuditThread: Audit thread already running");
        } else {
            LOG.debug("startAuditThread: starting audit from '{}'", path);

            executorService.execute(new Runnable() {
                
                public void run() {
                    status = "Running";
                    auditThreadShouldQuit.set(false);
                    auditThreadRunning.set(true);
                    auditThreadNodeCount.set(0);
                    auditThreadAssetsSeen.set(0);
                    auditThreadAssetsFixed.set(0);
                    
                    try {
                        Session session = repository.login(new SimpleCredentials("admin", adminPassword.toCharArray()));

                        Node rootNode = session.getNode(path);
                        walkTree(rootNode, fix);   
                        
                        if (fix && session.hasPendingChanges()) {
                			LOG.info("run: final save");
                			session.save();
                		}
  
                    } catch (Exception e) {
                        LOG.error("Caught error walking tree : " + e.getMessage(), e);
                        status = "Failed : " + e.getMessage();
                    }
                    
                    status = "Done";
                    auditThreadRunning.set(false);
                    auditThreadShouldQuit.set(false);

                }
                
                private void walkTree(Node node, boolean fix) throws Exception {
                    
                    if(auditThreadShouldQuit.get()) return;

                    NodeIterator nodeIter = node.getNodes();
                    while (nodeIter.hasNext()) {

                    	Node nextNode = nodeIter.nextNode();
                		LOG.debug("walkTree: at node {}", nextNode.getPath());

                    	auditThreadNodeCount.incrementAndGet();
                    	
                    	if (nextNode.isNodeType(JcrConstants.NT_FOLDER)) {
            				walkTree(nextNode, fix);

            			} else if (nextNode.isNodeType(DamConstants.NT_DAM_ASSET)) {
            				inspectSubAssets(nextNode, fix);
            			}	
                    }
                }
            });
        }
    }
    
	public void inspectSubAssets(Node assetNode, boolean fix) throws Exception {
		LOG.debug("inspectSubAssets: Inspecting subassets of asset {}", assetNode.getPath());
		if (assetNode.hasNode("jcr:content") && assetNode.getNode("jcr:content").hasNode("metadata")) {
		Node subAssetsNode = assetNode.getNode("jcr:content").getNode("metadata");
            if (subAssetsNode.hasProperty("prism:expirationDate") && subAssetsNode.getProperty("prism:expirationDate").getType() == 1) {
                auditThreadAssetsSeen.incrementAndGet();
                try {
                    calendar.setTime(curFormater.parse(subAssetsNode.getProperty("prism:expirationDate").getString()));
                }catch (ParseException e) {
                    calendar.setTime(curFormater.parse(defaultDateStr));
                }
                if (fix) {
                    subAssetsNode.setProperty("prism:expirationDate", calendar);
                    auditThreadAssetsFixed.incrementAndGet();
                }
            }
		}

		if (fix && assetNode.getSession().hasPendingChanges() && auditThreadAssetsFixed.get() % 1000 == 0) {
			LOG.info("inspectSubAssets: saving 1000 fixed subassets");
			assetNode.getSession().save();
		}
	}

    @Override
    public void stopAuditThread() {
        auditThreadShouldQuit.set(true);;
    }

}