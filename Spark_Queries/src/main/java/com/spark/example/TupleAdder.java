package com.spark.example;

import java.io.Serializable;

import scala.Tuple2;

public class TupleAdder extends Tuple2<Long, Long> implements Serializable {

	private static final long serialVersionUID = 1L;

	public TupleAdder(Long _1, Long _2) {
		super(_1, _2);
		// TODO Auto-generated constructor stub
	}
	
	public TupleAdder add(TupleAdder TA) {
		return new TupleAdder(this._1+TA._1,this._2+TA._2);
	}
	public double Average() {
		return (double)((double)this._1/(double)this._2);
	}

}
