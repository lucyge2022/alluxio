package alluxio.stage;

import alluxio.exception.StageOverloadException;
import alluxio.util.ThreadFactoryUtils;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RingBufferStage {
    private String mStageName;
    Disruptor<DefaultStageEvent> disruptor;

    public RingBufferStage(int ringBufferSize, String stageName, boolean isDaemon) {
        mStageName = stageName;
//        RingBuffer<DefaultStageEvent> ringBuffer = RingBuffer.create(
//                ProducerType.MULTI, , ringBufferSize, new BlockingWaitStrategy());
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(8, 64, 0,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(64 * 1024),
                ThreadFactoryUtils.build(stageName + "-%d", isDaemon));
        disruptor = new Disruptor<DefaultStageEvent>(
                DefaultStageEvent.EVENT_FACTORY,
                ringBufferSize,
                tpe,
//                ThreadFactoryUtils.build(stageName + "-%d", isDaemon),
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        DefaultEventHandler[] handlers = new DefaultEventHandler[8];
        for (int i=0;i<8;i++) {
            handlers[i] = new DefaultEventHandler();
        }
        disruptor.handleEventsWith(handlers);
        disruptor.setDefaultExceptionHandler(new DefaultStageExceptionHandler());
        disruptor.start();
    }

    public void submit(AlluxioStageTask stageTask) throws StageOverloadException {
        boolean published = disruptor.getRingBuffer().tryPublishEvent(
                (event, sequence) -> event.setStageTask(stageTask));
        if (!published) {
            throw new StageOverloadException(String.format("Stage[%s] is overloaded, capacity:%d",
                    mStageName, disruptor.getRingBuffer().getBufferSize()));
        }
    }

    public void dumpStageStats() {

    }

    public static void main(String[] args) {
        RingBufferStage stage = new RingBufferStage(16, "STAGE", false);
        try {
            Random random = new Random();
            for (int i=0;i<10;i++) {
                stage.submit(new AlluxioStageTask() {
                    @Override
                    public void execute() {
                        try {
                            System.out.println(Thread.currentThread().getName()+ " Starting sleeping 1s...");
                            int j = 0;
                            while(j++ < 100) {
                                Thread.sleep(1000);
                            }
                        } catch (Exception ex) {
                        }
                    }
                    @Override
                    public void handleStageException(Throwable e) {

                    }
                });
            }
        } catch (StageOverloadException e) {
            e.printStackTrace();
        }
    }

}

class DefaultEventHandler implements EventHandler<DefaultStageEvent> {

    @Override
    public void onEvent(DefaultStageEvent event, long l, boolean b) throws Exception {
        AlluxioStageTask stageTask = event.getStageTask();
        stageTask.beforeExecute();
        stageTask.execute();
        stageTask.afterExecute();
    }
}

class DefaultStageExceptionHandler implements ExceptionHandler<DefaultStageEvent> {

    public void handleEventException(Throwable ex, long sequence, DefaultStageEvent event) {
        event.getStageTask().handleStageException(ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        System.err.println("Unable to start stage.");
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        System.err.println("Unable to shutdown stage.");
    }
}
