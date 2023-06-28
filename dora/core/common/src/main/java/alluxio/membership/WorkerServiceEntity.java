package alluxio.membership;

import alluxio.grpc.GrpcUtils;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.MoreObjects;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class WorkerServiceEntity extends ServiceEntity {
  enum State {
    JOINED,
    AUTHORIZED,
    DECOMMISSIONED
  }
  WorkerNetAddress mAddress;
  State mState = State.JOINED;
  int mGenerationNum = -1;

  public WorkerServiceEntity() {
  }

  public WorkerNetAddress getWorkerNetAddress() {
    return mAddress;
  }

  public WorkerServiceEntity(WorkerNetAddress addr) {
    super(CommonUtils.hashAsStr(addr.dumpMainInfo()));
    mAddress = addr;
    mState = State.AUTHORIZED;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("WorkerId", getServiceEntityName())
        .add("WorkerAddr", mAddress.toString())
        .add("State", mState.toString())
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerServiceEntity anotherO = (WorkerServiceEntity) o;
    return mAddress.equals(anotherO) &&
        getServiceEntityName().equals(anotherO.getServiceEntityName());
  }

  public void serialize(DataOutputStream dos) throws IOException {
    super.serialize(dos);
    dos.writeInt(mState.ordinal());
    byte[] serializedArr = GrpcUtils.toProto(mAddress).toByteArray();
    dos.writeInt(serializedArr.length);
    dos.write(serializedArr);
  }

  public void deserialize(DataInputStream dis) throws IOException {
    super.deserialize(dis);
    mState = State.values()[dis.readInt()];
    int byteArrLen = dis.readInt();
    byte[] byteArr = new byte[byteArrLen];
    dis.read(byteArr, 0, byteArrLen);
    mAddress = GrpcUtils.fromProto(alluxio.grpc.WorkerNetAddress.parseFrom(byteArr));
  }
}
