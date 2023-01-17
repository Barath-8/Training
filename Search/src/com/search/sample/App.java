package com.search.sample;

import java.util.Scanner;

public class App {
	InMemoryCache cache = new InMemoryCache();
	
	void view() {
		String key = "";
		Scanner sc = new Scanner(System.in);
		
		while (true) {
			System.out.println("\n\nIf want to exit type \"quit123\"");
			System.out.print("\nEnter Search Key : ");
			key = sc.nextLine();
			
			if(key.equals("quit123"))
				break;
			
			long start = System.currentTimeMillis();
			SearchData sd = getData(key);
			long end = System.currentTimeMillis();
			System.out.println(sd);
			System.out.println("Time Taken : " + (end - start) + " Milliseconds");

		}
		
		sc.close();
	}
	
	SearchData getData(String key) {
		
		SearchData data = cache.get(key);
		
		if(data==null) {
			data = DbConnection.getInstance().search(key);
			
			if(data != null) {
				cache.put(data);
			}
			
		}
		
		return data;
	}

	public static void main(String[] args) {
		new App().view();
	}

}
