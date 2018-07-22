package other;

import java.util.concurrent.TimeUnit;

public class RunTest {

	public static void main(String[] args) {

		new RunTest().ttt();
		//System.gc();
		while (true){

			try {
				TimeUnit.SECONDS.sleep(11);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	private void  ttt(){
		FinalTest finalTest = new FinalTest();
		System.out.println(finalTest.toString());
		System.out.println(finalTest.avroUtil.toString());
		System.out.println(finalTest.avroUtil1.toString());
	}

}
