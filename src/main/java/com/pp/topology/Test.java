package com.pp.topology;

import java.io.File;

public class Test {

	public static void main(String[] args) {
		String filePath = "src/main/resources/word.txt";
		File file = new File(filePath);
		System.out.println(file.exists());
	}
}
