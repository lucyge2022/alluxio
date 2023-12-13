package alluxio.worker.ucx;

import com.google.common.base.Preconditions;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucs.UcsConstants;

import java.nio.ByteBuffer;

public class UcxMemoryPool {

  // Unpooled allocating
  public static UcpMemory allocateMemory(long length, int memType) {
    Preconditions.checkArgument(memType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST,
        "Currently only support allocating direct memory(buffer)");
    // alloc + register =>
    // check ucp_mem_map for behavior
    // https://openucx.readthedocs.io/en/master/api/
    // file/ucp_8h.html?highlight=ucp_mem_map
    // #_CPPv411ucp_mem_map13ucp_context_hPK20ucp_mem_map_params_tP9ucp_mem_h
    ByteBuffer directBuf = ByteBuffer.allocateDirect((int)length);
    UcpMemory allocMem = registerMemory(UcxUtils.getAddress(directBuf), length);
    return allocMem;
  }

  public static UcpMemory registerMemory(long addr, long length) {
    UcpMemory registerdMem = UcpServer.sGlobalContext.memoryMap(new UcpMemMapParams()
        .setAddress(addr).setLength(length).nonBlocking());
    return registerdMem;
  }

}
