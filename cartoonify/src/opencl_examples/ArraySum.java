package opencl_examples;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;

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

public class ArraySum {
	 /**
     * The source code of the OpenCL program to execute
     */
    private static final String arraySumSource =
        "__kernel void                          "+
        "arraySum(__global const int *a,        "+
        "             __global const int *b,    "+
        "             __global int *c)          "+
        "{                                      "+
        "    int gid = get_global_id(0);        "+
        "    c[gid] = a[gid] + b[gid];          "+
        "}";
    
  	
	/**
	 * main arguments: the length of array workgroupsize 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		/**if (args.length != 2) {
			System.err.println("Usage: ArraySum length workgroupsize");
			System.exit(1);
		}*/
	
		final int length = 1024;//Integer.parseInt(args[0]);
		final int workgroupsize = 256;//Integer.parseInt(args[1]);
		
		// we can list all available platforms and devices.
		for (CLPlatform p : JavaCL.listPlatforms()) {
			System.out.println("CLPlatform: " + p.getName() + " from " + p.getVendor());
			for (CLDevice dev : p.listAllDevices(false)) {
			    if(dev.getType().toString().indexOf("GPU") !=-1){
					System.out.println("************************************GPU**************************************");
				}
				else{
					System.out.println("************************************CPU**************************************");
				}
				Method[] methods = CLDevice.class.getMethods(); 
				for(Method m: methods){
					String methodName = m.getName();
					if(methodName.startsWith("get")){
						if(m.getParameterCount() == 0){
				   		  System.out.println(methodName + ": " + m.invoke(dev));
						}
				   	}
				}
		
			}
			
		}

		
		//CLContext context = JavaCL.createBestContext(DeviceFeature.GPU);
		//CLContext context = JavaCL.createBestContext(DeviceFeature.CPU);
		// choose the platform and device with the most compute units regardless of Device Type
		CLContext context = JavaCL.createBestContext();
		CLDevice[] devices = context.getDevices();
		for(int i=0;i<devices.length;i++){
			System.out.println("\n\nThe best Device " + i+":"+devices[i]);
		}
		
		//Create an OpenCL queue on the first device of this context.
		CLQueue queue = context.createDefaultQueue();	
			  
		// Allocate OpenCL-hosted memory for inputs and output
		CLBuffer<Integer> memIn1 = context.createIntBuffer(Usage.Input, length);		
		CLBuffer<Integer> memIn2 = context.createIntBuffer(Usage.Input, length);		
		CLBuffer<Integer> memOut = context.createIntBuffer(Usage.Output, length);

		//allocate input arrays on the host
		Pointer<Integer> a = Pointer.allocateInts(length);
		Pointer<Integer> b = Pointer.allocateInts(length);
		//Fill the arrays with some value 
		for (int i = 0; i < length; i++) {
			a.setIntAtIndex(i,i); //a[0]=0 , a[1]=1, a[2]=2 ...
			b.setIntAtIndex(i,i); //b[0]=0 , b[1]=1, b[2]=2 ...
		}
		
		//move the data to the device memory, blocking
		memIn1.write(queue, a, true, (CLEvent)null);
		memIn2.write(queue, b, true, (CLEvent)null);	
		
			
		CLProgram program = context.createProgram(arraySumSource).build();
		CLKernel kernel = program.createKernel("arraySum", memIn1, memIn2, memOut);
						
		final long time0 = System.nanoTime();
				
		kernel.enqueueNDRange(queue, new int[] {length },new int[] {workgroupsize});
	
		// Wait for all operations to be performed
		queue.finish();
		final long time1 = System.nanoTime();
		System.out.println("Done in " + (time1 - time0) / 1000
				+ " microseconds");

		// Copy the OpenCL-hosted output array back to RAM
		// We could do this via map;take-local-copy;unmap, but read does all
		// that for us.
		Pointer<Integer> output = memOut.read(queue);
	
	    for (int i = 0; i < 20; i++) {
			 System.out.print("["+output.get(i)+"] ");
		}
		 System.out.println("");
		
	}
	
}
