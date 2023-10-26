package alluxio.worker.ucx;

import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucs.UcsConstants;

public class UcxMemoryPool {

  // Unpooled allocating
  public static UcpMemory allocateMemory(long length, int memType) {
    // alloc + register =>
    // check ucp_mem_map for behavior
    // https://openucx.readthedocs.io/en/master/api/file/ucp_8h.html?highlight=ucp_mem_map#_CPPv411ucp_mem_map13ucp_context_hPK20ucp_mem_map_params_tP9ucp_mem_h
    UcpMemMapParams memMapParams = new UcpMemMapParams()
        .allocate()
        .setLength(length)
        .setMemoryType(memType);
    UcpMemory allocMem = UcpServer.sGlobalContext.memoryMap(memMapParams);
    return allocMem;
  }

}
