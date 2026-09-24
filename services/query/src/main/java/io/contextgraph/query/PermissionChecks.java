package io.contextgraph.query;

import io.contextgraph.security.*;

interface PermissionChecks {
  void workspace(SecurityContext context, String permission);
  boolean allowed(SecurityContext context, String resourceId);
  default void require(SecurityContext context,String resourceId) {
    if(!allowed(context,resourceId)) throw new SecurityException("Access revoked or denied");
  }
  static PermissionChecks using(AccessControl access) {
    return new PermissionChecks() {
      public void workspace(SecurityContext c,String p){access.requireWorkspace(c,p);}
      public boolean allowed(SecurityContext c,String r){return access.allowed(c,r,"view");}
      public void require(SecurityContext c,String r){access.require(c,r,"view");}
    };
  }
}
