package alluxio.stage;

import com.lmax.disruptor.EventFactory;

public class DefaultStageEvent {
    private Object context_;
    private AlluxioStageTask mStageTask;
    public final static EventFactory EVENT_FACTORY
            = () -> new DefaultStageEvent();

    public void setStageTask(AlluxioStageTask stageTask) {
        mStageTask = stageTask;
    }

    public AlluxioStageTask getStageTask() {
        return mStageTask;
    }
}