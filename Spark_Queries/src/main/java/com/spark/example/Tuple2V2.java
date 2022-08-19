package com.spark.example;

import scala.Tuple2;

public class Tuple2V2<T> extends Tuple2<String, T> implements Comparable<Tuple2V2<T>> {

	private static final long serialVersionUID = 1L;

	public Tuple2V2(String _1, T _2) {
		super(_1, _2);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compareTo(Tuple2V2<T> arg) {
		// TODO Auto-generated method stub
		return this._1().compareTo(arg._1());
	}

}
