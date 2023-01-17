package com.search.sample;

import java.util.HashMap;
import java.util.Map;

public class InMemoryCache {

	class Node {
		Node next, prev;
		SearchData data;

		Node(SearchData data) {
			this.data = data;
		}
	}

	Node head = new Node(null);
	Node tail = new Node(null);
	Map<String, Node> map = new HashMap<>();
	int capacity = 3;
	
	public InMemoryCache() {
		head.next=tail;
		tail.prev=head;
	}

	public SearchData get(String key) {

		if (map.containsKey(key)) {
			Node node = map.get(key);
			remove(node);
			insert(node);
			return node.data;
		} else {
			return null;
		}
	}

	public void put(SearchData data) {
		if (map.containsKey(data.getKey())) {
			remove(map.get(data.getKey()));
		}
		if (map.size() == capacity) {
			remove(tail.prev);
		}
		insert(new Node(data));
	}

	private void insert(Node node) {

		map.put(node.data.getKey(), node);
		Node headNext = head.next;
		head.next = node;
		node.prev = head;
		node.next = headNext;
		headNext.prev = node;

	}

	private void remove(Node node) {
		map.remove(node.data.getKey(), node);
		node.prev.next = node.next;
		node.next.prev = node.prev;
	}

}
