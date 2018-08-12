package opencl_examples;

import org.bridj.Pointer;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLContext;
import com.nativelibs4java.opencl.CLDevice;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLKernel;
import com.nativelibs4java.opencl.CLPlatform;
import com.nativelibs4java.opencl.CLProgram;
import com.nativelibs4java.opencl.CLQueue;
import com.nativelibs4java.opencl.JavaCL;
import com.nativelibs4java.opencl.CLMem.MapFlags;
import com.nativelibs4java.opencl.CLMem.Usage;
import com.nativelibs4java.opencl.CLPlatform.DeviceFeature;

public class MultipleQueue {

    /**
     * The source code of the OpenCL program to execute
     */
    private static final String source =
                    "__kernel void                           "+
                    "arraySum(__global int *a)               "+
                    "{                                       "+
                    "    int gid = get_global_id(0);         "+
                    "    a[gid] = a[gid] + a[gid];           "+
                    "}                                       "+
                    "                                        "+
                    "__kernel void                           "+
                    "arrayMultiply(__global int *a)          "+
                    "{                                       "+
                    "    int gid = get_global_id(0);         "+
                    "    a[gid] = a[gid] * a[gid];           "+
                    "}                                       "+
                    "                                        "+
                    "__kernel void                           "+
                    "arrayIncrement(__global int *a)         "+
                    "{                                       "+
                    "    int gid = get_global_id(0);         "+
                    "    a[gid] = a[gid] + 1;                "+
                    "}";


    /**
     *  no arguments required
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        final int length = 1<<24;//2^24
        final int workgroupsize = 64;//the preferred group size multiple on my machine

        // choose the platform and the best GPU device
        CLContext context = JavaCL.createBestContext(DeviceFeature.GPU);

        System.out.println("best context has device[0]=" + context.getDevices()[0]);


        //create three in-order queques
        CLQueue queue = context.createDefaultQueue();
        CLQueue queue2 = context.createDefaultQueue();
        CLQueue queue3 = context.createDefaultQueue();

        // Allocate OpenCL-hosted memory for inputs and output
        CLBuffer<Integer> memInOut = context.createIntBuffer(Usage.InputOutput, length);

        // Map input buffers to populate them with some data
        Pointer<Integer> a = memInOut.map(queue, MapFlags.ReadWrite);
        // Fill the mapped input buffers with some value
        for (int i = 0; i < length; i++) {
            a.setIntAtIndex(i,i); //a[0]=0 a[1] = 1 a[2] = 2 a[3] = 3
        }

        //Unmap input buffers
        memInOut.unmap(queue, a);


        CLProgram program = context.createProgram(source).build();
        CLKernel sumkernel = program.createKernel("arraySum", memInOut);
        CLKernel multiplykernel = program.createKernel("arrayMultiply",memInOut);
        CLKernel incrementKernel = program.createKernel("arrayIncrement", memInOut);


        final long time0 = System.nanoTime();


        /**in-order single queue
         sumkernel.enqueueNDRange(queue, new int[] {length },	new int[] { workgroupsize });
         multiplykernel.enqueueNDRange(queue, new int[] {length },	new int[] { workgroupsize });
         incrementKernel.enqueueNDRange(queue, new int[] {length },	new int[] { workgroupsize });
         **/

        /** use multiple queues (unsynchronized)
         sumkernel.enqueueNDRange(queue, new int[] {length },	new int[] { workgroupsize });
         multiplykernel.enqueueNDRange(queue2, new int[] {length },	new int[] { workgroupsize });
         incrementKernel.enqueueNDRange(queue3, new int[] {length },	new int[] { workgroupsize });
         **/

        /** use multiple queues (synchronized) **/
        CLEvent sumE = sumkernel.enqueueNDRange(queue, new int[] {length },	new int[] { workgroupsize });
        CLEvent mulE = multiplykernel.enqueueNDRange(queue2, new int[] {length },	new int[] { workgroupsize },new CLEvent[]{sumE});
        CLEvent incE = incrementKernel.enqueueNDRange(queue3, new int[] {length },	new int[] { workgroupsize },new CLEvent[]{mulE});


        //Execution begins when the user executes a synchronizing command,
        //such as clFlush or clWaitForEvents.
        //Enqueuing several commands before flushing can enable the host CPU
        //to batch together the command submission, which can reduce launch overhead.
        queue2.flush();
        queue3.flush();
        queue.flush();


        // Wait for all operations to be performed
        //changing the order would get different results
        queue.finish();
        queue2.finish();
        queue3.finish();

        final long time1 = System.nanoTime();
        System.out.println("Done in " + (time1 - time0) / 1000
                + " microseconds");

        // Copy the OpenCL-hosted output array back to RAM
        // We could do this via map;take-local-copy;unmap, but read does all
        // that for us.
        Pointer<Integer> output = memInOut.read(queue);

        //correct results
        //input:     [0] [1] [2]  [3]  [4] ....
        //add:       [0] [2] [4]  [6]  [8] ....
        //multiply:  [0] [4] [16] [36] [64] ....
        //increment: [1] [5] [17] [37] [65] [101] [145] [197] [257] [325] [401] [485] [577] [677] [785] [901] [1025] [1157] [1297] [1445]
        for (int i = 0; i < length; i++) {
            System.out.print("["+output.get(i)+"] ");
        }
    }
}
