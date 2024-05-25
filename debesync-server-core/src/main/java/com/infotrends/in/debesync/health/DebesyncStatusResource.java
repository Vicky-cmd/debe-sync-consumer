package com.infotrends.in.debesync.health;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.health.Liveness;

import java.util.Map;

@Named("DebesyncStatusResource")
@Path("/status")
public class DebesyncStatusResource {

    @Inject
    @Liveness
    protected ConsumerSyncHealth consumerSyncHealth;

    @GET
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> hello() {
        return consumerSyncHealth.status();
    }
}
