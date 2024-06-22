package com.learning.sparkbyexamples.algorithms;

import java.util.Scanner;

public class Utils {

	public static void waitForExit() {
		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNext()) {

			// if the next is a Int,
			// print found and the Int
			if (scanner.hasNextInt()) {
				System.out.println("Found Int value :" + scanner.nextInt() + " so exiting...");
				System.exit(0);
			}

			// if no Int is found,
			// print "Not Found:" and the token
			else {
				System.out.println("Not found Int value :" + scanner.next());
			}
		}
		scanner.close();
	}
}
